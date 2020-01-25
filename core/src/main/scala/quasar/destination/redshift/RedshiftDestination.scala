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
import quasar.api.table.{ColumnType, TableColumn, TableName}
import quasar.blobstore.paths.{BlobPath, PathElem}
import quasar.blobstore.services.PutService
import quasar.connector.MonadResourceErr

import cats.data.ValidatedNel
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._

import doobie._
import doobie.implicits._

import eu.timepit.refined.auto._

import fs2.{Stream, gzip}

import org.slf4s.Logging

import java.util.UUID

import scalaz.NonEmptyList

import shims._

final class RedshiftDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
  put: PutService[F],
  config: RedshiftConfig,
  xa: Transactor[F]) extends Destination[F] with Logging {

  def destinationType: DestinationType =
    RedshiftDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F]] = NonEmptyList(csvSink)

  private val csvSink: ResultSink[F] = ResultSink.csv[F](RenderConfig.Csv()) {
    case (path, columns, bytes) => Stream.eval({
      for {
        suffix <- Sync[F].delay(UUID.randomUUID().toString)
        freshName = s"reform-$suffix.gz"
        compressed = bytes.through(gzip.compress(1024))
        uploadPath = BlobPath(List(PathElem(freshName)))
        _ <- put((uploadPath, compressed))
      } yield ()
    })
  }

  private def createTableQuery(tableName: TableName, columns: NonEmptyList[Fragment]): Fragment =
    fr"CREATE TABLE" ++ Fragment.const(tableName.name) ++
      Fragments.parentheses(columns.intercalate(fr",")) ++ fr"with nopartition"

  private def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs
      .map(err => s"Column of type ${err.show} is not supported by Redshift")
      .intercalate(", ")

  private def ensureValidColumns(columns: List[TableColumn]): Either[String, NonEmptyList[Fragment]] =
    for {
      cols0 <- columns.toNel.toRight("No columns specified")
      cols <- cols0.traverse(mkColumn(_)).toEither.leftMap(errs =>
        s"Some column types are not supported: ${mkErrorString(errs.asScalaz)}")
    } yield cols.asScalaz

  private def mkColumn(c: TableColumn): ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToRedshift(c.tpe).map(Fragment.const(c.name) ++ _)

  private def columnTypeToRedshift(ct: ColumnType.Scalar)
      : ValidatedNel[ColumnType.Scalar, Fragment] =
    ct match {
      case ColumnType.Null => fr0"SMALLINT".validNel
      case ColumnType.Boolean => fr0"BOOLEAN".validNel
      case lt @ ColumnType.LocalTime => lt.invalidNel
      case ot @ ColumnType.OffsetTime => ot.invalidNel
      case ColumnType.LocalDate => fr0"DATE".validNel
      case od @ ColumnType.OffsetDate => od.invalidNel
      case ColumnType.LocalDateTime => fr0"TIMESTAMP".validNel
      case ColumnType.OffsetDateTime => fr0"TIMESTAMPTZ".validNel
      case i @ ColumnType.Interval => i.invalidNel
      case ColumnType.Number => fr0"FLOAT".validNel
      case ColumnType.String => fr0"TEXT".validNel
    }

}
