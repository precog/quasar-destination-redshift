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

import quasar.api.Column
import quasar.api.push.OffsetKey
import quasar.api.resource._
import quasar.connector._
import quasar.connector.destination.{ResultSink, WriteMode}, ResultSink.{UpsertSink, AppendSink}
import quasar.connector.render.RenderConfig

import cats.Applicative
import cats.data._
import cats.effect.{Concurrent, Resource, Sync}
import cats.effect.concurrent.Ref
import cats.implicits._

import fs2.{Stream, Pipe, Chunk}

import skolems.∀

private[redshift] trait Flow[F[_]] {
  type Stage
  def stage(inp: Stream[F, Byte]): Resource[F, Stage]
  def ingest(s: Stage): F[Unit]
  def delete(ids: IdBatch): Stream[F, Unit]
}

object Flow {
  private type FlowColumn = Column[RedshiftType]

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

    def ofUpsert(args: UpsertSink.Args[RedshiftType]): Args = new Args {
      def path = args.path
      def columns = args.columns
      def writeMode = args.writeMode
      def idColumn = args.idColumn.some
    }

    def ofAppend(args: AppendSink.Args[RedshiftType]): Args = new Args {
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

    def flowSinks(implicit F: Concurrent[F]): NonEmptyList[ResultSink[F, RedshiftType]] =
      NonEmptyList.of(ResultSink.create(create), ResultSink.upsert(upsert), ResultSink.append(append))

    private def create(path: ResourcePath, cols: NonEmptyList[FlowColumn])(implicit F: Applicative[F])
        : (RenderConfig[Byte], Pipe[F, Byte, Unit]) = {
      val args = Args.ofCreate(path, cols)
      (render, in => Stream.resource {
        for {
          flow <- flowR(args)
          s <- flow.stage(in)
          _ <- Resource.liftF(flow.ingest(s))
        } yield ()
      })
    }

    private def upsert(upsertArgs: UpsertSink.Args[RedshiftType])(implicit F: Concurrent[F])
        : (RenderConfig[Byte], ∀[Consume[DataEvent[Byte, *], *]]) = {
      val args = Args.ofUpsert(upsertArgs)
      val consume = ∀[Consume[DataEvent[Byte, *], *]](upsertPipe(args))
      (render, consume)
    }

    private def append(appendArgs: AppendSink.Args[RedshiftType])(implicit F: Concurrent[F])
        : (RenderConfig[Byte], ∀[Consume[AppendEvent[Byte, *], *]]) = {
      val args = Args.ofAppend(appendArgs)
      val consume = ∀[Consume[AppendEvent[Byte, *], *]](upsertPipe(args))
      (render, consume)
    }

    private def upsertPipe[A](args: Args)(implicit F: Concurrent[F])
        : Pipe[F, DataEvent[Byte, OffsetKey.Actual[A]], OffsetKey.Actual[A]] = { events =>
      for {
        flow <- Stream.resource(flowR(args))
        staged <- Stream.eval(Ref.of[F, List[flow.Stage]](List()))
        finalizers <- Stream.eval(Ref.of[F, F[Unit]](().pure[F]))
        offset <- events
          .through(Event.fromDataEvent[F, OffsetKey.Actual[A]])
          .through(ingest[OffsetKey.Actual[A]](flow)(staged, finalizers))
      } yield offset
    }

    private def ingest[A](flow: Flow[F])(staged: Ref[F, List[flow.Stage]], fin: Ref[F, F[Unit]])(implicit F: Sync[F])
        : Pipe[F, Event[F, A], A] = { events =>
      def handleEvent: Pipe[F, Event[F, A], Option[A]] = _ flatMap {
        case Event.Create(stream) => Stream.eval {
          for {
            (s, f) <- flow.stage(stream).allocated
            _ <- staged.update(s :: _)
            _ <- fin.update(_ >> f)
          } yield none[A]
        }
        case Event.Delete(ids) =>
          flow.delete(ids).mapChunks(_ => Chunk(none[A]))
        case Event.Commit(offset) => Stream.eval {
          for {
            ss <- staged.getAndSet(List())
            _ <- ss.traverse_(flow.ingest)
          } yield offset.some
        }
      }

      events.through(handleEvent).unNone.onFinalize {
        fin.get.flatten
      }
    }
  }
}
