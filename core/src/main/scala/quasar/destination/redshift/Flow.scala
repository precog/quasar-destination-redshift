/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.destination.redshift

import slamdata.Predef._

import quasar.api.{Column, ColumnType}
import quasar.api.push.OffsetKey
import quasar.api.resource._
import quasar.connector._
import quasar.connector.destination.{ResultSink, WriteMode}, ResultSink.{UpsertSink, AppendSink}
import quasar.connector.render.RenderConfig

import cats.data._
import cats.effect.{Concurrent, Resource}
import cats.implicits._

import fs2.{Stream, Pipe, Chunk}

import skolems.∀

private[redshift] trait Flow[F[_]] {
  def ingest: Pipe[F, Byte, Unit]
  def delete(ids: IdBatch): Stream[F, Unit]
}

object Flow {
  private type FlowColumn = Column[ColumnType.Scalar]

  sealed trait Args {
    def path: ResourcePath
    def columns: NonEmptyList[FlowColumn]
    def writeMode: WriteMode
    def idColumn: Option[Column[_]]
  }

  object Args {
    def ofCreate(p: ResourcePath, cs: NonEmptyList[FlowColumn]): Args = new Args {
      def path = p
      def columns = cs
      def writeMode = WriteMode.Replace
      def idColumn = None
    }

    def ofUpsert(args: UpsertSink.Args[ColumnType.Scalar]): Args = new Args {
      def path = args.path
      def columns = args.columns
      def writeMode = args.writeMode
      def idColumn = args.idColumn.some
    }

    def ofAppend(args: AppendSink.Args[ColumnType.Scalar]): Args = new Args {
      def path = args.path
      def columns = args.columns
      def writeMode = args.writeMode
      def idColumn = args.pushColumns.primary
    }
  }

  trait Sinks[F[_]] {

    type Consume[E[_], A] =
      Pipe[F, E[OffsetKey.Actual[A]], OffsetKey.Actual[A]]

    def flowR(args: Args): Resource[F, Flow[F]]

    val render: RenderConfig[Byte]

    def flowSinks(implicit F: Concurrent[F]): NonEmptyList[ResultSink[F, ColumnType.Scalar]] =
      NonEmptyList.of(ResultSink.create(create), ResultSink.upsert(upsert), ResultSink.append(append))

    private def create(path: ResourcePath, cols: NonEmptyList[FlowColumn])
        : (RenderConfig[Byte], Pipe[F, Byte, Unit]) = {
      val args = Args.ofCreate(path, cols)
      (render, in => for {
        flow <- Stream.resource(flowR(args))
        _ <- Stream.emit(Event.Create(in)).through(ingest[Unit](flow))
      } yield ())
    }

    private def upsert(upsertArgs: UpsertSink.Args[ColumnType.Scalar])(implicit F: Concurrent[F])
        : (RenderConfig[Byte], ∀[Consume[DataEvent[Byte, *], *]]) = {
      val args = Args.ofUpsert(upsertArgs)
      val consume = ∀[Consume[DataEvent[Byte, *], *]](upsertPipe(args))
      (render, consume)
    }

    private def append(appendArgs: AppendSink.Args[ColumnType.Scalar])(implicit F: Concurrent[F])
        : (RenderConfig[Byte], ∀[Consume[AppendEvent[Byte, *], *]]) = {
      val args = Args.ofAppend(appendArgs)
      val consume = ∀[Consume[AppendEvent[Byte, *], *]](upsertPipe(args))
      (render, consume)
    }

    private def upsertPipe[A](args: Args)(implicit F: Concurrent[F])
        : Pipe[F, DataEvent[Byte, OffsetKey.Actual[A]], OffsetKey.Actual[A]] = { events =>
      for {
        flow <- Stream.resource(flowR(args))
        offset <- events
          .through(Event.fromDataEvent[F, OffsetKey.Actual[A]])
          .through(ingest[OffsetKey.Actual[A]](flow))
      } yield offset
    }

    private def ingest[A](flow: Flow[F]): Pipe[F, Event[F, A], A] = { events =>
      def handleEvent: Pipe[F, Event[F, A], Option[A]] = _ flatMap {
        case Event.Create(stream) =>
          flow.ingest(stream).mapChunks(_ => Chunk(none[A]))
        case Event.Delete(ids) =>
          flow.delete(ids).mapChunks(_ => Chunk(none[A]))
        case Event.Commit(offset) =>
          Stream.emit(offset.some)
      }

      events.through(handleEvent).unNone
    }
  }
}
