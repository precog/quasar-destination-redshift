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

import argonaut._, Argonaut._, ArgonautScalaz._

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.util.Read

import fs2.{Pull, Stream}

import java.time._

import org.specs2.matcher.MatchResult
import org.specs2.execute.AsResult
import org.specs2.specification.core.{Fragment => SFragment}

import quasar.EffectfulQSpec
import quasar.api.Column
import quasar.api.destination._
import quasar.api.push.OffsetKey
import quasar.api.resource._
import quasar.contrib.scalaz.MonadError_
import quasar.connector._
import quasar.connector.destination._

import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

import scalaz.-\/
import scalaz.syntax.show._

import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}
import shapeless.ops.record.{Keys, Values}
import shapeless.record._
import shapeless.syntax.singleton._

import shims.applicativeToScalaz

object RedshiftDestinationSpec extends EffectfulQSpec[IO] with CsvSupport {

  skipAllIf(!testsEnabled)

  "initialization" should {
    "fail with malformed config when not decodable" >>* {
      val cfg = Json("malformed" := true)

      dest(cfg)(r => IO.pure(r match {
        case Left(DestinationError.MalformedConfiguration(_, c, _)) =>
          c must_=== jEmptyObject

        case _ => ko("Expected a malformed configuration")
      }))
    }

    "fail when unable to connect to database" >>* {
      val cfg = config(url = "jdbc:redshift://localhost:1234/foobar")

      dest(cfg)(r => IO.unit).attempt.map(_ must beLeft)

    }
  }

  "seek sinks (upsert and append)" should {
    "write after commit" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", RedshiftType.VARCHAR(512))), WriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)
        }
    }

    "write two chunks with a single commit" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Create(List(
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", RedshiftType.VARCHAR(512))), WriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)

          offsets must_== List(OffsetKey.Actual.string("commit1"))
        }
    }

    "not write without a commit" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Create(List(("x" ->> "quz") :: ("y" ->> "corge") :: HNil)),
          UpsertEvent.Commit("commit1"),
          UpsertEvent.Create(List(("x" ->> "baz") :: ("y" ->> "qux") :: HNil)))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", RedshiftType.VARCHAR(512))), WriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "quz" :: "corge" :: HNil)
          offsets must_== List(OffsetKey.Actual.string("commit1"))
        }
    }

    "commit twice in a row" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Commit("commit1"),
          UpsertEvent.Commit("commit2"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", RedshiftType.VARCHAR(512))), WriteMode.Replace, events)
        } yield {
          values must_== List("foo" :: "bar" :: HNil)
          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
    }

    "upsert updates rows with string typed primary key on append" >>
      upsertOnly[String :: String :: HNil ] { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"))

        val append =
          Stream(
            UpsertEvent.Delete(Ids.StringIds(List("foo"))),
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "check0") :: HNil,
                ("x" ->> "bar") :: ("y" ->> "check1") :: HNil)),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values1, offsets1) <- consumer(tbl, Some(Column("x", RedshiftType.VARCHAR(512))), WriteMode.Replace, events)
          (values2, offsets2) <- consumer(tbl, Some(Column("x", RedshiftType.VARCHAR(512))), WriteMode.Append, append)
        } yield {
          offsets1 must_== List(OffsetKey.Actual.string("commit1"))
          offsets2 must_== List(OffsetKey.Actual.string("commit2"))
          Set(values1:_*) must_== Set("foo" :: "bar" :: HNil, "baz" :: "qux" :: HNil)
          Set(values2:_*) must_== Set("baz" :: "qux" :: HNil, "foo" :: "check0" :: HNil, "bar" :: "check1" :: HNil)
        }
      }

    "upsert deletes rows with long typed primary key on append" >>
      upsertOnly[Int :: String :: HNil] { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> 40) :: ("y" ->> "bar") :: HNil,
                ("x" ->> 42) :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"))

        val append =
          Stream(
            UpsertEvent.Delete(Ids.LongIds(List(40))),
            UpsertEvent.Create(
              List(
                ("x" ->> 40) :: ("y" ->> "check") :: HNil)),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values1, offsets1) <- consumer(tbl, Some(Column("x", RedshiftType.INTEGER)), WriteMode.Replace, events)
          (values2, offsets2) <- consumer(tbl, Some(Column("x", RedshiftType.INTEGER)), WriteMode.Append, append)
        } yield {
          Set(values1:_*) must_== Set(40 :: "bar" :: HNil, 42 :: "qux" :: HNil)
          Set(values2:_*) must_== Set(40 :: "check" :: HNil, 42 :: "qux" :: HNil)
          offsets1 must_== List(OffsetKey.Actual.string("commit1"))
          offsets2 must_== List(OffsetKey.Actual.string("commit2"))
        }
      }

    "upsert empty deletes without failing" >>
      upsertOnly[String :: String :: HNil] { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.StringIds(List())),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(Column("x", RedshiftType.VARCHAR(512))), WriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)

          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
      }

    "creates table and then appends" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events1 =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Commit("commit1"))

      val events2 =
        Stream(
          UpsertEvent.Create(List(("x" ->> "bar") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit2"))

      for {
        tbl <- freshTableName

        _ <- consumer(tbl, toOpt(Column("x", RedshiftType.VARCHAR(512))), WriteMode.Replace, events1)

        (values, _) <- consumer(tbl, Some(Column("x", RedshiftType.VARCHAR(512))), WriteMode.Append, events2)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "bar" :: "qux" :: HNil)
        }
    }
}

  "csv sink" should {
    "reject empty paths with NotAResource" >>* {
      csv(config()) { sink =>
        val p = ResourcePath.root()
        val (_, pipe) = sink.consume(p, NonEmptyList.one(Column("a", RedshiftType.BOOL)))
        val r = pipe(Stream.empty).compile.drain

        MRE.attempt(r).map(_ must beLike {
          case -\/(ResourceError.NotAResource(p2)) => p2 must_=== p
        })
      }
    }

    "reject paths with > 1 segments with NotAResource" >>* {
      csv(config()) { sink =>
        val p = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
        val (_, pipe) = sink.consume(p, NonEmptyList.one(Column("a", RedshiftType.BOOL)))
        val r = pipe(Stream.empty).compile.drain

        MRE.attempt(r).map(_ must beLike {
          case -\/(ResourceError.NotAResource(p2)) => p2 must_=== p
        })
      }
    }

    "quote table names to prevent injection" >>* {
      csv(config()) { sink =>
        val recs = List(("x" ->> 2.3) :: ("y" ->> 8.1) :: HNil)

        drainAndSelect(
          TestConnectionUrl,
          "foobar; drop table really_important; create table haha",
          sink,
          Stream.emits(recs)
        ).map(_ must_=== recs)
      }
    }

    "support table names containing double quote" >>* {
      csv(config()) { sink =>
        val recs = List(("x" ->> 934.23) :: ("y" ->> 1234424.123456) :: HNil)

        drainAndSelect(
          TestConnectionUrl,
          """the "table" name""",
          sink,
          Stream.emits(recs)
        ).map(_ must_=== recs)
      }
    }

    "quote column names to prevent injection" >>* {
      mustRoundtrip(("from nowhere; drop table super_mission_critical; select *" ->> 42) :: HNil)
    }

    "support columns names containing a double quote" >>* {
      mustRoundtrip(("""the "column" name""" ->> 76) :: HNil)
    }

    "create an empty table when input is empty" >>* {
      csv(config()) { sink =>
        type R = Record.`"a" -> String, "b" -> Int, "c" -> LocalDate`.T

        for {
          tbl <- freshTableName
          r <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream.empty.covaryAll[IO, R])
        } yield r must beEmpty
      }
    }

    "create a row for each line of input" >>* mustRoundtrip(
      ("foo" ->> 1) :: ("bar" ->> "baz1") :: ("quux" ->> 34.34234) :: HNil,
      ("foo" ->> 2) :: ("bar" ->> "baz2") :: ("quux" ->> 35.34234) :: HNil,
      ("foo" ->> 3) :: ("bar" ->> "baz3") :: ("quux" ->> 36.34234) :: HNil)


    "roundtrip Boolean" >>* mustRoundtrip(("min" ->> true) :: ("max" ->> false) :: HNil)
    "roundtrip Short" >>* mustRoundtrip(("min" ->> Short.MinValue) :: ("max" ->> Short.MaxValue) :: HNil)
    "roundtrip Int" >>* mustRoundtrip(("min" ->> Int.MinValue) :: ("max" ->> Int.MaxValue) :: HNil)

    "roundtrip String" >>* {
      IO(scala.util.Random.nextString(32)) flatMap { s =>
        mustRoundtrip(("value" ->> s) :: HNil)
      }
    }

    "create new schema if user-defined schema provided" >>* {
      val schema = "myschema"

      csv(config(schema = Some(schema))) { sink =>
        val r1 = ("x" ->> 1) :: ("y" ->> "two") :: ("z" ->> "3.00001") :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r1), schema)
        } yield (res1 must_=== List(r1))
      }
    }
  }

  val DM = RedshiftDestinationModule

  def TestConnectionUrl: String =
    scala.sys.env.get("REDSHIFT_JDBC") getOrElse ""

  def TestBucket: String =
    scala.sys.env.get("REDSHIFT_BUCKET") getOrElse ""

  def TestAccessKey: String =
    scala.sys.env.get("REDSHIFT_ACCESS_KEY") getOrElse ""

  def TestSecretKey: String =
    scala.sys.env.get("REDSHIFT_SECRET_KEY") getOrElse ""

  def TestRegion: String =
    scala.sys.env.get("REDSHIFT_REGION") getOrElse ""

  def User: String =
    scala.sys.env.get("REDSHIFT_USER") getOrElse ""

  def Password: String =
    scala.sys.env.get("REDSHIFT_PASSWORD") getOrElse ""

  def testsEnabled: Boolean =
    List(TestConnectionUrl, TestBucket, TestAccessKey, TestSecretKey, TestRegion, User, Password)
      .forall(x => x =!= "")

  implicit val CS: ContextShift[IO] = IO.contextShift(global)

  implicit val TM: Timer[IO] = IO.timer(global)

  implicit val MRE: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  object renderRow extends renderForCsv(DM.RedshiftRenderConfig)

  def hygienicIdent(s: String): String =
    RedshiftFlow.escape(s)

  def randomAlphaNum[F[_]: Sync](size: Int): F[String] =
    Sync[F].defer(Random.alphanumeric.take(size).mkString)

  val freshTableName: IO[String] =
    randomAlphaNum[IO](6).map(suffix => s"redshift_test${suffix}")

  def runDb[F[_]: Async: ContextShift, A](fa: ConnectionIO[A], uri: String = TestConnectionUrl): F[A] =
    fa.transact(Transactor.fromDriverManager[F]("com.amazon.redshift.jdbc42.Driver", s"$uri;UID=$User;PWD=$Password"))

  def config(url: String = TestConnectionUrl, schema: Option[String] = None): Json =
    ("jdbcUri" := url) ->:
    ("schema" := schema) ->:
    ("bucket" :=
      ("bucket" := TestBucket) ->:
        ("accessKey" := TestAccessKey) ->:
        ("secretKey" := TestSecretKey) ->:
      ("region" := TestRegion) ->:
      jEmptyObject) ->:
    ("password" := Password) ->:
    ("user" := User) ->:
    ("authorization" :=
      ("type" := "keys") ->:
        ("accessKey" := TestAccessKey) ->:
        ("secretKey" := TestSecretKey) ->:
      ("region" := TestRegion) ->:
      jEmptyObject) ->:
    jEmptyObject

  def csv[A](cfg: Json)(f: ResultSink.CreateSink[IO, RedshiftType, Byte] => IO[A]): IO[A] =
    dest(cfg) {
      case Left(err) =>
        IO.raiseError(new RuntimeException(err.shows))

      case Right(dst) =>
        dst.sinks.toList
          .collectFirst { case c @ ResultSink.CreateSink(_) => c }
          .map(s => f(s.asInstanceOf[ResultSink.CreateSink[IO, RedshiftType, Byte]]))
          .getOrElse(IO.raiseError(new RuntimeException("No CSV sink found!")))
    }

  def upsertCsv[A](cfg: Json)(f: ResultSink.UpsertSink[IO, RedshiftType, Byte] => IO[A]): IO[A] =
    dest(cfg) {
      case Left(err) =>
        IO.raiseError(new RuntimeException(err.shows))

      case Right(dst) =>
        dst.sinks.toList
          .collectFirst { case c @ ResultSink.UpsertSink(_) => c }
          .map(s => f(s.asInstanceOf[ResultSink.UpsertSink[IO, RedshiftType, Byte]]))
          .getOrElse(IO.raiseError(new RuntimeException("No upsert CSV sink found!")))
    }

  def appendSink[A](cfg: Json)(f: ResultSink.AppendSink[IO, RedshiftType] => IO[A]): IO[A] =
    dest(cfg) {
      case Left(err) =>
        IO.raiseError(new RuntimeException(err.shows))

      case Right(dst) =>
        dst.sinks.toList
          .collectFirst { case c @ ResultSink.AppendSink(_) => c }
          .map(s => f(s.asInstanceOf[ResultSink.AppendSink[IO, RedshiftType]]))
          .getOrElse(IO.raiseError(new RuntimeException("No append CSV sink found!")))
    }

  def dest[A](cfg: Json)(f: Either[DM.InitErr, Destination[IO]] => IO[A]): IO[A] =
    DM.destination[IO](cfg, _ => _ => Stream.empty).use(f)

  trait Consumer[A]{
    def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
        table: String,
        idColumn: Option[Column[RedshiftType]],
        writeMode: WriteMode,
        records: Stream[IO, UpsertEvent[R]])(
        implicit
        read: Read[A],
        keys: Keys.Aux[R, K],
        values: Values.Aux[R, V],
        getTypes: Mapper.Aux[asColumnType.type, V, T],
        rrow: Mapper.Aux[renderRow.type, V, S],
        ktl: ToList[K, String],
        vtl: ToList[S, String],
        ttl: ToList[T, RedshiftType])
        : IO[(List[A], List[OffsetKey.Actual[String]])]
  }

  object Consumer {
    def upsert[A](cfg: Json, connectionUri: String, schema: String = "public"): Resource[IO, Consumer[A]] = {
      val rsink: Resource[IO, ResultSink.UpsertSink[IO, RedshiftType, Byte]] =
        DM.destination[IO](cfg, _ => _ => Stream.empty) evalMap {
          case Left(err) =>
            IO.raiseError(new RuntimeException(err.shows))
          case Right(dst) =>
            val optSink = dst.sinks.toList.collectFirst { case c @ ResultSink.UpsertSink(_) => c }
            optSink match {
              case Some(s) => s.asInstanceOf[ResultSink.UpsertSink[IO, RedshiftType, Byte]].pure[IO]
              case None => IO.raiseError(new RuntimeException("No upsert sink found"))
            }
        }
      rsink map { sink => new Consumer[A] {
        def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
            table: String,
            idColumn: Option[Column[RedshiftType]],
            writeMode: WriteMode,
            records: Stream[IO, UpsertEvent[R]])(
            implicit
            read: Read[A],
            keys: Keys.Aux[R, K],
            values: Values.Aux[R, V],
            getTypes: Mapper.Aux[asColumnType.type, V, T],
            rrow: Mapper.Aux[renderRow.type, V, S],
            ktl: ToList[K, String],
            vtl: ToList[S, String],
            ttl: ToList[T, RedshiftType])
            : IO[(List[A], List[OffsetKey.Actual[String]])] = {

        val createSchema = fr"CREATE SCHEMA IF NOT EXISTS" ++
          Fragment.const(hygienicIdent(schema))

        val tblFragment = Fragment.const0(hygienicIdent(schema)) ++ fr0"." ++ Fragment.const(hygienicIdent(table))

        for {
          tableColumns <- columnsOf(records, renderRow, idColumn).compile.lastOrError

          colList = (idColumn.get :: tableColumns).map(k =>
            Fragment.const(hygienicIdent(k.name))).intercalate(fr",")

          dst = ResourcePath.root() / ResourceName(table)

          _ <- runDb[IO, Int](createSchema.update.run, connectionUri)

          offsets <- toUpsertCsvSink(
            dst, sink, idColumn.get, writeMode, renderRow, records).compile.toList

          q = fr"SELECT" ++ colList ++ fr"FROM" ++ tblFragment

          rows <- runDb[IO, List[A]](q.query[A].to[List], connectionUri)

        } yield (rows, offsets)
      }}}
    }

    def append[A](cfg: Json, connectionUri: String, schema: String = "public"): Resource[IO, Consumer[A]] = {
      val rsink: Resource[IO, ResultSink.AppendSink[IO, RedshiftType]] =
        DM.destination[IO](cfg, _ => _ => Stream.empty) evalMap {
          case Left(err) =>
            IO.raiseError(new RuntimeException(err.shows))
          case Right(dst) =>
            val optSink = dst.sinks.toList.collectFirst { case c @ ResultSink.AppendSink(_) => c }
            optSink match {
              case Some(s) => s.asInstanceOf[ResultSink.AppendSink[IO, RedshiftType]].pure[IO]
              case None => IO.raiseError(new RuntimeException("No append sink found"))
            }
        }
      rsink map { sink => new Consumer[A] {
        def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
            table: String,
            idColumn: Option[Column[RedshiftType]],
            writeMode: WriteMode,
            records: Stream[IO, UpsertEvent[R]])(
            implicit
            read: Read[A],
            keys: Keys.Aux[R, K],
            values: Values.Aux[R, V],
            getTypes: Mapper.Aux[asColumnType.type, V, T],
            rrow: Mapper.Aux[renderRow.type, V, S],
            ktl: ToList[K, String],
            vtl: ToList[S, String],
            ttl: ToList[T, RedshiftType])
            : IO[(List[A], List[OffsetKey.Actual[String]])] = {

        val createSchema = fr"CREATE SCHEMA IF NOT EXISTS" ++
          Fragment.const(hygienicIdent(schema))

        val tblFragment = Fragment.const0(hygienicIdent(schema)) ++ fr0"." ++ Fragment.const(hygienicIdent(table))

        for {
          tableColumns <- columnsOf(records, renderRow, idColumn).compile.lastOrError

          colList = (idColumn.toList ++ tableColumns).map(k =>
            Fragment.const(hygienicIdent(k.name))).intercalate(fr",")

          dst = ResourcePath.root() / ResourceName(table)

          _ <- runDb[IO, Int](createSchema.update.run, connectionUri)

          offsets <- toAppendCsvSink(
            dst, sink, idColumn, writeMode, renderRow, records).compile.toList

          q = fr"SELECT" ++ colList ++ fr"FROM" ++ tblFragment

          rows <- runDb[IO, List[A]](q.query[A].to[List], connectionUri)

        } yield (rows, offsets)
      }
    }}}
  }

  object upsertOnly {
    def apply[A]: PartiallyApplied[A] = new PartiallyApplied[A]

    final class PartiallyApplied[A] {
      def apply[R: AsResult](f: Consumer[A] => IO[R]): SFragment = {
        "upsert" >>*
          Consumer.upsert[A](config(), TestConnectionUrl).use(f)
        "upsert custom schema" >>*
          Consumer.upsert[A](config(schema = "myschema".some), TestConnectionUrl, schema = "myschema").use(f)
      }
    }
  }
  object appendAndUpsert {
    def apply[A]: PartiallyApplied[A] = new PartiallyApplied[A]

    final class PartiallyApplied[A] {
      type MkOption = Column[RedshiftType] => Option[Column[RedshiftType]]
      def apply[R: AsResult](
          f: (MkOption, Consumer[A]) => IO[R])
          : SFragment = {
        "upsert" >>*
          Consumer.upsert[A](config(), TestConnectionUrl).use(f(Some(_), _))
        "upsert custom schema" >>*
          Consumer.upsert[A](config(schema = "myschema".some), TestConnectionUrl, schema = "myschema").use(f(Some(_), _))
        "append" >>*
          Consumer.append[A](config(), TestConnectionUrl).use(f(Some(_), _))
        "append custom schema" >>*
          Consumer.append[A](config(schema = "myschema".some), TestConnectionUrl, schema = "myschema").use(f(Some(_), _))
        "no-id" >>*
          Consumer.append[A](config(), TestConnectionUrl).use(f(x => None, _))
        "no-id custom schema" >>*
          Consumer.append[A](config(schema = "myschema".some), TestConnectionUrl, schema = "myschema").use(f(x => None, _))
      }
    }
  }

  object idOnly {
    def apply[A]: PartiallyApplied[A] = new PartiallyApplied[A]

    final class PartiallyApplied[A] {
      type MkOption = Column[RedshiftType] => Option[Column[RedshiftType]]
      def apply[R: AsResult](
          f: (MkOption, Consumer[A]) => IO[R])
          : SFragment = {
        "upsert" >>*
          Consumer.upsert[A](config(), TestConnectionUrl).use(f(Some(_), _))
        "upsert custom schema" >>*
          Consumer.upsert[A](config(schema = "myschema".some), TestConnectionUrl, schema = "myschema").use(f(Some(_), _))
        "append" >>*
          Consumer.append[A](config(), TestConnectionUrl).use(f(Some(_), _))
        "append custom schema" >>*
          Consumer.append[A](config(schema = "myschema".some), TestConnectionUrl, schema = "myschema").use(f(Some(_), _))
      }
    }
  }

  def drainAndSelect[F[_]: Async: ContextShift, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      connectionUri: String,
      table: String,
      sink: ResultSink.CreateSink[F, RedshiftType, Byte],
      records: Stream[F, R],
      schema: String = "public")(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      read: Read[V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      rrow: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      vtl: ToList[S, String],
      ttl: ToList[T, RedshiftType])
      : F[List[V]] =
    drainAndSelectAs[F, V](connectionUri, table, sink, records, schema)

  object drainAndSelectAs {
    def apply[F[_], A]: PartiallyApplied[F, A] =
      new PartiallyApplied[F, A]

    final class PartiallyApplied[F[_], A]() {
      def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
          connectionUri: String,
          table: String,
          sink: ResultSink.CreateSink[F, RedshiftType, Byte],
          records: Stream[F, R],
          schema: String = "public")(
          implicit
          async: Async[F],
          cshift: ContextShift[F],
          read: Read[A],
          keys: Keys.Aux[R, K],
          values: Values.Aux[R, V],
          getTypes: Mapper.Aux[asColumnType.type, V, T],
          rrow: Mapper.Aux[renderRow.type, V, S],
          ktl: ToList[K, String],
          vtl: ToList[S, String],
          ttl: ToList[T, RedshiftType])
          : F[List[A]] = {

        val dst = ResourcePath.root() / ResourceName(table)

        val go = records.pull.peek1 flatMap {
          case Some((r, rs)) =>
            val colList =
              r.keys.toList
                .map(k => Fragment.const(hygienicIdent(k)))
                .intercalate(fr",")

            val createSchema = fr"CREATE SCHEMA IF NOT EXISTS" ++
              Fragment.const(hygienicIdent(schema))

            val q = fr"SELECT" ++
              colList ++
              fr"FROM" ++
              Fragment.const(hygienicIdent(schema)) ++
              fr0"." ++
              Fragment.const(hygienicIdent(table))

            val run = for {
              _ <- runDb[F, Int](createSchema.update.run, connectionUri)
              _ <- toCsvSink(dst, sink, renderRow, rs).compile.drain
              rows <- runDb[F, List[A]](q.query[A].to[List], connectionUri)
            } yield rows

            Pull.eval(run).flatMap(Pull.output1)

          case None =>
            Pull.done
        }

        go.stream.compile.last.map(_ getOrElse Nil)
      }
    }
  }

  def loadAndRetrieve[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      record: R,
      records: R*)(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      read: Read[V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      rrow: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      vtl: ToList[S, String],
      ttl: ToList[T, RedshiftType])
      : IO[List[V]] =
    randomAlphaNum[IO](6) flatMap { tableSuffix =>
      val table = s"redshift_dest_test${tableSuffix}"
      val cfg = config(url = TestConnectionUrl)
      val rs = record +: records

      csv(cfg)(drainAndSelect(TestConnectionUrl, table, _, Stream.emits(rs)))
    }

  def mustRoundtrip[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      record: R,
      records: R*)(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      read: Read[V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      rrow: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      vtl: ToList[S, String],
      ttl: ToList[T, RedshiftType])
      : IO[MatchResult[List[V]]] =
    loadAndRetrieve(record, records: _*)
      .map(_ must containTheSameElementsAs(record +: records))
}
