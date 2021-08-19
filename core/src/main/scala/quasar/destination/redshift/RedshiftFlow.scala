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
import quasar.api.resource._
import quasar.blobstore.paths.{BlobPath, PathElem}
import quasar.blobstore.s3.{
  AccessKey,
  Bucket,
  SecretKey,
  Region
}
import quasar.blobstore.services.{DeleteService, PutService}
import quasar.connector._
import quasar.connector.destination.WriteMode

import cats.Applicative
import cats.data.NonEmptyList
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{compression, Stream}

import org.slf4s.Logging

import java.util.UUID

import shims._

final class RedshiftFlow[F[_]: ConcurrentEffect: ContextShift: Timer: MonadResourceErr](
    deleteService: DeleteService[F],
    putService: PutService[F],
    config: RedshiftConfig,
    schemaFragment: Option[Fragment],
    tblFragment: Fragment,
    xa: Transactor[F],
    writeModeRef: Ref[F, WriteMode],
    args: Flow.Args)
    extends Flow[F] with Logging {

  type Stage = BlobPath

  def stage(bytes: Stream[F, Byte]): Resource[F, Stage] = {
    val compressed = bytes.through(compression.gzip(bufferSize = 1024 * 32))

    val make = for {
      suffix <- Sync[F].delay(UUID.randomUUID().toString)
      freshName = s"reform-$suffix.gz"
      uploadPath = BlobPath(List(PathElem(freshName)))
      _ <- putService((uploadPath, compressed))
    } yield uploadPath

    Resource.make(make)(deleteService(_).void)
  }

  def delete(ids: IdBatch): Stream[F, Unit] = args.idColumn.traverse_ { idCol =>

    val strs: Array[String] = ids match {
      case IdBatch.Strings(values, size) => values.take(size).map(x => "'" + x.replace("'", "''") + "'")
      case IdBatch.Longs(values, size) => values.take(size).map(_.toString)
      case IdBatch.Doubles(values, size) => values.take(size).map(_.toString)
      case IdBatch.BigDecimals(values, size) => values.take(size).map(_.toString)
    }

    // The limit is 16Mb, we split on 4Mb, no need to count `DELETE FROM`
    @scala.annotation.tailrec
    def group(inp: Array[String], accum: (Int, List[List[String]]), len: Int, ix: Int)
        : List[List[String]] =
      if (ix >= len) accum._2
      else {
        val elem = inp(ix)
        val elemSize = elem.getBytes.length
        val resultSize = accum._1 + elemSize
        val newAccum = if (resultSize >= RedshiftFlow.DeleteQueryLimit) {
          (elemSize, List(elem) :: accum._2)
        }
        else (resultSize, accum._2 match {
          case hd :: tail => (elem :: hd) :: tail
          case other => List(List(elem))
        })

        group(inp, newAccum, len, ix + 1)
      }

    val grouped = group(strs, (0, List()), strs.length, 0)

    val deleteFromFragment =
      fr"DELETE FROM" ++
      tblFragment ++
      fr"WHERE" ++
      Fragment.const(RedshiftFlow.escape(idCol.name))

    Stream
      .emits(grouped)
      .map({ (x: List[String]) =>
        deleteFromFragment ++
        fr"IN" ++
        Fragments.parentheses(x.map(Fragment.const(_)).intercalate(fr","))
      })
      .evalMap({ (fr: Fragment) =>
        debug[F](s"DELETE QUERY: ${fr.update.sql}") >>
        fr.update.run.transact(xa).void
      })
  }

  def ingest(uploaded: Stage): F[Unit] = {
    val colFragments = args.columns map mkColumn
    for {
      writeMode <- writeModeRef.get
      _ <- writeMode match {
        case WriteMode.Replace =>
          createNewTable(colFragments).transact(xa) >>
          writeModeRef.set(WriteMode.Append)
        case WriteMode.Append =>
          ().pure[F]
      }
      _ <- copyInto(uploaded, config.uploadBucket.bucket, args.columns, config.authorization)
    } yield ()
  }

  private def copyInto(
      blob: Stage,
      bucket: Bucket,
      cols: NonEmptyList[Column[_]],
      authorization: Authorization): F[Unit] = {
    val columnsFragment =
      Fragments.parentheses(cols.map(x => Fragment.const(RedshiftFlow.escape(x.name))).intercalate(fr","))

    val s3PathFragment = {
      val path = blob.path.map(_.value).intercalate("/")
      Fragment.const(s"'s3://${bucket.value}/$path'")
    }

    val authFragment: Authorization => Fragment = {
      case Authorization.RoleARN(arn0, Region(region0)) =>
        val arn = removeSingleQuotes(arn0)
        val region = removeSingleQuotes(region0)
        Fragment.const(s"iam_role '$arn' region '$region'")
      case Authorization.Keys(AccessKey(accessKey0), SecretKey(secretKey0), Region(region0)) =>
        val accessKey = removeSingleQuotes(accessKey0)
        val secretKey = removeSingleQuotes(secretKey0)
        val region = removeSingleQuotes(region0)
        Fragment.const(s"access_key_id '$accessKey' secret_access_key '$secretKey' region '$region'")
    }

    val copyFragment: Authorization => Fragment = { a =>
      fr"COPY" ++
      tblFragment ++
      columnsFragment ++
      fr"FROM" ++
      s3PathFragment ++
      authFragment(authorization) ++
      fr"CSV" ++
      fr"GZIP" ++
      fr"EMPTYASNULL"
    }

    debug[F](s"COPY QUERY: ${copyFragment(redactAuth(authorization)).update.sql}") >>
    copyFragment(authorization).update.run.void.transact(xa)
  }

  private def removeSingleQuotes(inp: String): String =
    inp.replace("'", "")

  private def createNewTable(columns: NonEmptyList[Fragment]): ConnectionIO[Unit] = {
    dropTableIfExists >>
    createTable(columns)
  }

  private def dropTableIfExists: ConnectionIO[Unit] = {
    val fragment = fr"DROP TABLE IF EXISTS" ++ tblFragment
    debug[ConnectionIO](s"DROP QUERY: ${fragment.update.sql}") >>
    fragment.update.run.void
  }

  private def createTable(columns: NonEmptyList[Fragment]): ConnectionIO[Unit] = {
    val fragment =
      fr"CREATE TABLE" ++ tblFragment ++ Fragments.parentheses(columns.intercalate(fr","))
    debug[ConnectionIO](s"CREATE QUERY: ${fragment.update.sql}") >>
    fragment.update.run.void
  }


  private def mkColumn(c: Column[RedshiftType]): Fragment =
    Fragment.const(RedshiftFlow.escape(c.name)) ++ c.tpe.asSql

  private def debug[G[_]: Sync](msg: String): G[Unit] =
    Sync[G].delay(log.debug(msg))
}

object RedshiftFlow {
  val DeleteQueryLimit = 4 * 1024 * 1024

  def apply[F[_]: ConcurrentEffect: ContextShift: Timer: MonadResourceErr](
      deleteService: DeleteService[F],
      putService: PutService[F],
      config: RedshiftConfig,
      rxa: Resource[F, Transactor[F]],
      args: Flow.Args)
      : Resource[F, Flow[F]] = for {
    xa <- rxa
    tableName <- Resource.eval(ensureValidTableName[F](args.path))
    writeModeRef <- Resource.eval(Ref.of[F, WriteMode](args.writeMode))
  } yield {
    val nameFragment = Fragment.const(tableName)

    val schemaFragment = config.schema.map(x => Fragment.const0(escape(x)))

    val tblFragment = schemaFragment match {
      case None => nameFragment
      case Some(s) => s ++ fr0"." ++ nameFragment
    }
    new RedshiftFlow(
      deleteService,
      putService,
      config,
      schemaFragment,
      tblFragment,
      xa,
      writeModeRef,
      args)
  }

  private def ensureValidTableName[F[_]: Applicative: MonadResourceErr](r: ResourcePath): F[String] = r match {
    case file /: ResourcePath.Root => escape(file).pure[F]
    case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(r))
  }

  def escape(ident: String): String = {
    val escaped =
      ident
        .replace("\\", "")
        .replace(" ", "_")
        .replace("\"", "\"\"")

    s""""$escaped""""
  }
}
