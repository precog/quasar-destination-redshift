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

import quasar.connector._

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.implicits._

import fs2.{Pipe, Stream, Chunk}
import fs2.concurrent.Queue

sealed trait Event[+F[_], +A]

object Event {
  type Nothing1[A] = Nothing

  final case class Commit[A](value: A) extends Event[Nothing1, A]
  final case class Create[F[_]](value: Stream[F, Byte]) extends Event[F, Nothing]
  final case class Delete(value: IdBatch) extends Event[Nothing1, Nothing]

  def fromDataEvent[F[_]: Concurrent, A]: Pipe[F, DataEvent[Byte, A], Event[F, A]] = {
    def flush(elqRef: Ref[F, Option[Queue[F, Option[Chunk[Byte]]]]], resQ: Queue[F, Option[Event[F, A]]]): F[Unit] =
      elqRef.get.flatMap(_.traverse { q =>
        // When there is a queue (we're collecting inner stream)
        // Stop collecting
        q.enqueue1(None)
      }) >>
      // notify we're not collecting
      elqRef.set(None)

    def go(
        inp: Stream[F, DataEvent[Byte, A]],
        // result queue
        resQ: Queue[F, Option[Event[F, A]]],
        // element queue reference
        elqRef: Ref[F, Option[Queue[F, Option[Chunk[Byte]]]]])
        : Stream[F, Unit] = inp evalMap {
      case DataEvent.Create(chunk) => elqRef.get flatMap {
        case None => for {
          newQ <- Queue.unbounded[F, Option[Chunk[Byte]]]
          _ <- newQ.enqueue1(chunk.some)
          _ <- elqRef.set(newQ.some)
          _ <- resQ.enqueue1(Event.Create(newQ.dequeue.unNoneTerminate.flatMap(Stream.chunk)).some)
        } yield ()
        case Some(q) =>
          q.enqueue1(chunk.some)
      }
      case DataEvent.Delete(ids) =>
        flush(elqRef, resQ) >>
        resQ.enqueue1(Event.Delete(ids).some)
      case DataEvent.Commit(offset) =>
        flush(elqRef, resQ) >>
        resQ.enqueue1(Event.Commit(offset).some)
    }

    inp => for {
      resQ <- Stream.eval(Queue.bounded[F, Option[Event[F, A]]](256))
      elqRef <- Stream.eval(Ref.of[F, Option[Queue[F, Option[Chunk[Byte]]]]](None))
      results <- resQ.dequeue.unNoneTerminate.concurrently {
        val inpWithFinalizer = inp.onFinalize {
          elqRef.get.flatMap(_.traverse_(_.enqueue1(None))) >>
          resQ.enqueue1(None)
        }

        go(inpWithFinalizer, resQ, elqRef)
      }
    } yield results
  }
}
