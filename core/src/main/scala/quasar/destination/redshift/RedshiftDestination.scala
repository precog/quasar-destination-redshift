/*
 * Copyright 2014–2019 SlamData Inc.
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

import scala.{Stream => _, _}
import scala.Predef._

import quasar.api.destination.{Destination, DestinationType, ResultSink}
import quasar.api.push.RenderConfig
import quasar.api.resource._
import quasar.api.table.{ColumnType, TableColumn, TableName}
import quasar.blobstore.paths.{BlobPath, PathElem, PrefixPath}
import quasar.blobstore.services.{DeleteService, PutService}
import quasar.blobstore.s3.Bucket
import quasar.connector.{MonadResourceErr, ResourceError}

import cats.data.ValidatedNel
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._

import doobie._
import doobie.implicits._

import eu.timepit.refined.auto._

import fs2.{Stream, gzip}

import org.slf4s.Logging

import java.time.format.DateTimeFormatter
import java.util.UUID

import scalaz.NonEmptyList

import shims._

final class RedshiftDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
  deleteService: DeleteService[F],
  put: PutService[F],
  config: RedshiftConfig,
  xa: Transactor[F]) extends Destination[F] with Logging {

  private val RedshiftRenderConfig = RenderConfig.Csv(
    includeHeader = false,
    includeBom = false,
    // RedShift doesn't have time-only data types so we fake it by prepending an arbitrary date (1900-01-01)
    offsetDateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ssx"),
    offsetTimeFormat = DateTimeFormatter.ofPattern("'1900-01-01' HH:mm:ssx"),
    localDateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"),
    localTimeFormat = DateTimeFormatter.ofPattern("'1900-01-01' HH:mm:ss"),
    localDateFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd"))

  def destinationType: DestinationType =
    RedshiftDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F]] = NonEmptyList(csvSink)

  private val csvSink: ResultSink[F] = ResultSink.csv[F](RedshiftRenderConfig) {
    case (path, columns, bytes) => Stream.eval({
      for {
        cols <- Sync[F].fromEither(ensureValidColumns(columns).leftMap(new RuntimeException(_)))

        tableName <- ensureValidTableName(path)

        compressed = bytes.through(gzip.compress(1024))

        suffix <- Sync[F].delay(UUID.randomUUID().toString)

        prefix = s"reform-$suffix"
        freshName = s"$prefix/0.gz"

        uploadPath = BlobPath(List(PathElem(freshName)))

        _ <- put((uploadPath, compressed))

        dropQuery = dropTableQuery(tableName).update

        createQuery = createTableQuery(tableName, cols).update

        copyQuery = copyTableQuery(
          tableName,
          config.uploadBucket.bucket,
          PrefixPath(List(PathElem(prefix))),
          config.authorization).update

        _ <- debug(s"Drop table query: ${dropQuery.sql}")

        _ <- debug(s"Create table query: ${createQuery.sql}")

        _ <- debug(s"Copy table query: ${copyQuery.sql}")

        _ <- (dropQuery.run *> createQuery.run *> copyQuery.run).transact(xa)

      } yield ()
    })
  }

  private def ensureValidColumns(columns: List[TableColumn]): Either[String, NonEmptyList[Fragment]] =
    for {
      cols0 <- columns.toNel.toRight("No columns specified")
      cols <- cols0.traverse(mkColumn(_)).toEither.leftMap(errs =>
        s"Some column types are not supported: ${mkErrorString(errs.asScalaz)}")
    } yield cols.asScalaz

  private def ensureValidTableName(r: ResourcePath): F[TableName] =
    r match {
      case file /: ResourcePath.Root => TableName(file).pure[F]
      case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(r))
    }

  private def copyTableQuery(
    tableName: TableName,
    bucket: Bucket,
    blob: PrefixPath,
    auth: Authorization)
      : Fragment =
    fr"COPY" ++
       Fragment.const(tableName.name) ++
       fr"FROM" ++
       s3PathFragment(bucket, blob) ++
       authFragment(auth) ++
       fr"CSV" ++
       fr"GZIP" ++
       fr"EMPTYASNULL"

  private def s3PathFragment(bucket: Bucket, prefix: PrefixPath): Fragment = {
    val path = prefix.path.map(_.value).intercalate("/")

    Fragment.const(s"'s3://${bucket.value}/$path/'")
  }

  private def authFragment(auth: Authorization): Fragment = auth match {
    case Authorization.RoleARN(arn) =>
      Fragment.const(s"iam_role '$arn'")
    case Authorization.Keys(accessKey, secretKey) =>
      Fragment.const(s"access_key_id '${accessKey.value}' secret_access_key '${secretKey.value}'")
  }

  private def createTableQuery(tableName: TableName, columns: NonEmptyList[Fragment]): Fragment =
    fr"CREATE TABLE" ++ Fragment.const(tableName.name) ++
      Fragments.parentheses(columns.intercalate(fr","))

  private def dropTableQuery(tableName: TableName): Fragment =
    fr"DROP TABLE IF EXISTS" ++ Fragment.const(tableName.name)

  private def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs
      .map(err => s"Column of type ${err.show} is not supported by Redshift")
      .intercalate(", ")

  private def mkColumn(c: TableColumn): ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToRedshift(c.tpe).map(Fragment.const(s""""${c.name}"""") ++ _)

  private def columnTypeToRedshift(ct: ColumnType.Scalar)
      : ValidatedNel[ColumnType.Scalar, Fragment] =
    ct match {
      case ColumnType.Null => fr0"SMALLINT".validNel
      case ColumnType.Boolean => fr0"BOOLEAN".validNel
      case lt @ ColumnType.LocalTime => fr0"TIMESTAMP".validNel
      case ot @ ColumnType.OffsetTime => fr0"TIMESTAMPTZ".validNel
      case ColumnType.LocalDate => fr0"DATE".validNel
      case od @ ColumnType.OffsetDate => od.invalidNel
      case ColumnType.LocalDateTime => fr0"TIMESTAMP".validNel
      case ColumnType.OffsetDateTime => fr0"TIMESTAMPTZ".validNel
      case i @ ColumnType.Interval => i.invalidNel
      case ColumnType.Number => fr0"DECIMAL".validNel
      case ColumnType.String => fr0"TEXT".validNel
    }

  private def debug(msg: String): F[Unit] =
    Sync[F].delay(log.debug(msg))
}
