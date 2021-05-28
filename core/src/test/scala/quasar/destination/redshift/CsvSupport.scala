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

import cats.ApplicativeError
import cats.data.NonEmptyList
import cats.effect.Async
import cats.implicits._

import com.github.tototoshi.csv._

import cats.implicits._
import fs2._

import java.io.ByteArrayOutputStream
import java.time._

import qdata.time._
import quasar.api.{Column, ColumnType}
import quasar.api.resource.ResourcePath
import quasar.api.push.{PushColumns, OffsetKey}
import quasar.connector.{AppendEvent, IdBatch, DataEvent}
import quasar.connector.destination.ResultSink
import quasar.connector.render.RenderConfig
import quasar.connector.destination.{ResultSink, WriteMode => QWriteMode}

import scala.Float
import scala.collection.immutable.Seq

import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}
import shapeless.ops.record.{Keys, Values}
import shapeless.record._

trait CsvSupport {

  // Must remain in sync with Quasar's CSV output format to be representative.
  val QuasarCSVFormat: CSVFormat =
    new CSVFormat {
      val delimiter = ','
      val quoteChar = '"'
      val escapeChar = '"'
      val lineTerminator = "\r\n"
      val quoting = QUOTE_MINIMAL
      val treatEmptyLineAsNil = false
    }

  sealed trait UpsertEvent[+A] extends Product with Serializable
  sealed trait Ids extends Product with Serializable

  object Ids {
    case class StringIds(ids: List[String]) extends Ids
    case class LongIds(ids: List[Long]) extends Ids
  }

  object UpsertEvent {
    case class Create[A](records: List[A]) extends UpsertEvent[A]
    case class Delete(recordsIds: Ids) extends UpsertEvent[Nothing]
    case class Commit(value: String) extends UpsertEvent[Nothing]
  }

  // TODO: handle includeHeader == true
  def toCsvSink[F[_]: ApplicativeError[?[_], Throwable], P <: Poly1, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      dst: ResourcePath,
      sink: ResultSink.CreateSink[F, ColumnType.Scalar, Byte],
      renderRow: P,
      records: Stream[F, R])(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      renderValues: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      stl: ToList[S, String],
      ttl: ToList[T, ColumnType.Scalar])
      : Stream[F, Unit] = {

    val go = records.pull.peek1 flatMap {
      case Some((r, rs)) =>
        val rkeys = r.keys.toList
        val rtypes = r.values.map(asColumnType).toList
        val columns = rkeys.zip(rtypes).map((Column[ColumnType.Scalar] _).tupled)
        val encoded = rs.through(encodeCsvRecords[F, renderRow.type, R, V, S](renderRow))

        encoded.through(sink.consume(dst, NonEmptyList.fromListUnsafe(columns))._2).pull.echo

      case None => Pull.done
    }

    go.stream
  }

  def columnsOf[
    F[_]: Async, P <: Poly1, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
    events: Stream[F, UpsertEvent[R]],
    renderRow: P,
    idColumn: Option[Column[ColumnType.Scalar]])(
    implicit
    keys: Keys.Aux[R, K],
    values: Values.Aux[R, V],
    getTypes: Mapper.Aux[asColumnType.type, V, T],
    ktl: ToList[K, String],
    ttl: ToList[T, ColumnType.Scalar])
    : Stream[F, List[Column[ColumnType.Scalar]]] = {
    def go(inp: Stream[F, UpsertEvent[R]]): Pull[F, List[Column[ColumnType.Scalar]], Unit] = inp.pull.uncons1 flatMap {
      case Some((UpsertEvent.Create(records), tail)) => records.headOption match {
        case Some(r) =>
          val rkeys = r.keys.toList
          val rtypes = r.values.map(asColumnType).toList
          val columns = rkeys.zip(rtypes).map((Column[ColumnType.Scalar] _).tupled)
          Pull.output1(columns.filter(c => c.some =!= idColumn)) >> Pull.done
        case None =>
          Pull.done
      }
      case Some((_, tail)) =>
        go(tail)
      case _ =>
        Pull.done

    }
    go(events).stream
  }

  def toAppendCsvSink[F[_]: Async, P <: Poly1, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      dst: ResourcePath,
      sink: ResultSink.AppendSink[F, ColumnType.Scalar],
      idColumn: Option[Column[ColumnType.Scalar]],
      writeMode: QWriteMode,
      renderRow: P,
      events: Stream[F, UpsertEvent[R]])(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      renderValues: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      stl: ToList[S, String],
      ttl: ToList[T, ColumnType.Scalar])
      : Stream[F, OffsetKey.Actual[String]] = {

    val encoded: Stream[F, AppendEvent[Byte, OffsetKey.Actual[String]]] = events flatMap {
      case UpsertEvent.Create(records) => {
        Stream.emits(records)
          .covary[F]
          .through(
            encodeCsvRecordsToChunk[F, renderRow.type, R, V, S](renderRow))
          .map(DataEvent.Create(_))
      }

      case UpsertEvent.Commit(s) =>
        Stream(
          DataEvent.Commit(OffsetKey.Actual.string(s)))

      case UpsertEvent.Delete(_) =>
        Stream.eval_(Async[F].raiseError(new Exception("AppendSink can't handle delete events")))
    }

    type Consumed = ResultSink.AppendSink.Result[F] { type A = Byte }
    for {
      colList <- columnsOf(events, renderRow, idColumn)
      cols = idColumn match {
        case None => PushColumns.NoPrimary(NonEmptyList.fromListUnsafe(colList))
        case Some(i) => PushColumns.HasPrimary(List(), i, colList)
      }
      consumed = sink.consume.apply(
        ResultSink.AppendSink.Args(dst, cols, writeMode))
      res <- encoded.through(consumed.asInstanceOf[Consumed].pipe[String])
    } yield res
  }

  def toUpsertCsvSink[F[_]: Async, P <: Poly1, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      dst: ResourcePath,
      sink: ResultSink.UpsertSink[F, ColumnType.Scalar, Byte],
      idColumn: Column[ColumnType.Scalar],
      writeMode: QWriteMode,
      renderRow: P,
      events: Stream[F, UpsertEvent[R]])(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      renderValues: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      stl: ToList[S, String],
      ttl: ToList[T, ColumnType.Scalar])
      : Stream[F, OffsetKey.Actual[String]] = {

    val encoded: Stream[F, DataEvent[Byte, OffsetKey.Actual[String]]] = events flatMap {
      case UpsertEvent.Create(records) => {
        Stream.emits(records)
          .covary[F]
          .through(
            encodeCsvRecordsToChunk[F, renderRow.type, R, V, S](renderRow))
          .map(DataEvent.create[Byte, OffsetKey.Actual[String]])
      }

      case UpsertEvent.Commit(s) =>
        Stream(
          DataEvent.Commit(OffsetKey.Actual.string(s)))

      case UpsertEvent.Delete(Ids.StringIds(is)) =>
        Stream(
          DataEvent.Delete(IdBatch.Strings(is.toArray, is.length)))

      case UpsertEvent.Delete(Ids.LongIds(is)) =>
        Stream(
          DataEvent.Delete(IdBatch.Longs(is.toArray, is.length)))
    }

    columnsOf(events, renderRow, Some(idColumn)) flatMap { cols =>
      val (_, pipe) = sink.consume.apply(
        ResultSink.UpsertSink.Args(dst, idColumn, cols, writeMode))

      encoded.through(pipe[String])
    }
  }

  def encodeCsvRecordsToChunk[F[_]: Async, P <: Poly1, R <: HList, V <: HList, S <: HList](
      renderRow: P)(
      implicit
      values: Values.Aux[R, V],
      render: Mapper.Aux[renderRow.type, V, S],
      ltl: ToList[S, String])
      : Pipe[F, R, Chunk[Byte]] =
    encodeCsvRecords[F, P, R, V, S](renderRow).andThen(bytes =>
      bytes.chunks.fold(Chunk.empty[Byte])((l, r) => Chunk.concatBytes(Seq(l, r))))

  def encodeCsvRecords[F[_]: ApplicativeError[?[_], Throwable], P <: Poly1, R <: HList, V <: HList, S <: HList](
      renderRow: P)(
      implicit
      values: Values.Aux[R, V],
      render: Mapper.Aux[renderRow.type, V, S],
      ltl: ToList[S, String])
      : Pipe[F, R, Byte] =
    _.map(_.values.map(renderRow).toList).through(encodeCsvRows[F])

  class renderForCsv(cfg: RenderConfig.Csv) extends Poly1 {
    implicit val boolCase = at[Boolean](_.toString)

    implicit val localTimeCase = at[LocalTime](_.format(cfg.localTimeFormat))
    implicit val offsetTimeCase = at[OffsetTime](_.format(cfg.offsetTimeFormat))

    implicit val localDateCase = at[LocalDate](_.format(cfg.localDateFormat))
    implicit val offsetDateCase = at[OffsetDate](cfg.offsetDateFormat.format)

    implicit val localDateTimeCase = at[LocalDateTime](_.format(cfg.localDateTimeFormat))
    implicit val offsetDateTimeCase = at[OffsetDateTime](_.format(cfg.offsetDateTimeFormat))

    implicit val dateTimeIntervalCase = at[DateTimeInterval](_.toString)

    implicit val shortCase = at[Short](_.toString)
    implicit val intCase = at[Int](_.toString)
    implicit val longCase = at[Long](_.toString)
    implicit val floatCase = at[Float](_.toString)
    implicit val doubleCase = at[Double](_.toString)
    implicit val bigDecCase = at[BigDecimal](_.toString)

    implicit val charCase = at[Char](_.toString)
    implicit val stringCase = at[String](s => s)
  }

  object asColumnType extends Poly1 {
    implicit val boolCase = at[Boolean](_ => ColumnType.Boolean)

    implicit val localTimeCase = at[LocalTime](_ => ColumnType.LocalTime)
    implicit val offsetTimeCase = at[OffsetTime](_ => ColumnType.OffsetTime)

    implicit val localDateCase = at[LocalDate](_ => ColumnType.LocalDate)
    implicit val offsetDateCase = at[OffsetDate](_ => ColumnType.OffsetDate)

    implicit val localDateTimeCase = at[LocalDateTime](_ => ColumnType.LocalDateTime)
    implicit val offsetDateTimeCase = at[OffsetDateTime](_ => ColumnType.OffsetDateTime)

    implicit val dateTimeIntervalCase = at[DateTimeInterval](_ => ColumnType.Interval)

    implicit val shortCase = at[Short](_ => ColumnType.Number)
    implicit val intCase = at[Int](_ => ColumnType.Number)
    implicit val longCase = at[Long](_ => ColumnType.Number)
    implicit val floatCase = at[Float](_ => ColumnType.Number)
    implicit val doubleCase = at[Double](_ => ColumnType.Number)
    implicit val bigDecCase = at[BigDecimal](_ => ColumnType.Number)

    implicit val charCase = at[Char](_ => ColumnType.String)
    implicit val stringCase = at[String](_ => ColumnType.String)
  }

  ////

  private def encodeCsvRows[F[_]](implicit F: ApplicativeError[F, Throwable])
      : Pipe[F, Seq[String], Byte] =
    in => Stream.suspend {
      val os = new ByteArrayOutputStream
      val csvWriter = CSVWriter.open(os, "UTF-8")(QuasarCSVFormat)

      def drainBytes: Stream[F, Byte] = {
        val bs = os.toByteArray
        os.reset()
        Stream.chunk(Chunk.bytes(bs))
      }

      val process: Stream[F, Byte] =
        for {
          row <- in

          _ <- Stream.eval(F catchNonFatal {
            csvWriter.writeRow(row)
            csvWriter.flush()
          })

          b <- drainBytes
        } yield b

      val finish: Stream[F, Byte] =
        Stream.suspend {
          csvWriter.close()
          drainBytes
        }

      process ++ finish
    }
}

object CsvSupport extends CsvSupport
