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

import quasar.api.{ColumnType, Label}
import quasar.api.destination.DestinationType
import quasar.api.push.{TypeCoercion, SelectedType, TypeIndex}
import quasar.api.push.param.Actual

import quasar.blobstore.services.{DeleteService, PutService}
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Constructor, Destination, ResultSink}

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import doobie._

import monocle.Prism

import skolems.∃

final class RedshiftDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
  deleteService: DeleteService[F],
  put: PutService[F],
  config: RedshiftConfig,
  xa: Transactor[F]) extends Destination[F] with Flow.Sinks[F] {

  import RedshiftType._

  type Type = RedshiftType
  type TypeId = RedshiftTypeId

  val typeIdOrdinal: Prism[Int, TypeId] =
    Prism((i: Int) => RedshiftDestination.OrdinalMap.get(i))(_.ordinal)

  val typeIdLabel: Label[TypeId] =
    Label.label[TypeId](_.toString)

  def construct(id: TypeId): Either[Type, Constructor[Type]] = id match {
    case tpe: RedshiftTypeId.SelfIdentified => Left(tpe)
    case hk: RedshiftTypeId.HigherKinded => Right(hk.constructor)
  }

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[TypeId] = tpe match {
    case ColumnType.Boolean =>
      satisfied(BOOL)
    case ColumnType.LocalTime =>
      satisfied(TIME)
    case ColumnType.LocalDate =>
      satisfied(DATE)
    case ColumnType.LocalDateTime =>
      satisfied(TIMESTAMP)
    case ColumnType.OffsetTime =>
      satisfied(TIMETZ)
    case ColumnType.OffsetDate =>
      TypeCoercion.Unsatisfied(List(ColumnType.LocalDate), None)
    case ColumnType.OffsetDateTime =>
      satisfied(TIMESTAMPTZ)
    case ColumnType.String =>
      satisfied(CHAR, VARCHAR, TEXT, BPCHAR)
    case ColumnType.Number =>
      satisfied(FLOAT, INTEGER, DECIMAL, FLOAT4, BIGINT, INTEGER, SMALLINT)
    case ColumnType.Interval =>
      TypeCoercion.Unsatisfied(List(), None)
    case ColumnType.Null =>
      TypeCoercion.Unsatisfied(List(), None)
  }

  private def satisfied(t: TypeId, ts: TypeId*) =
    TypeCoercion.Satisfied(NonEmptyList(t, ts.toList))

  override def defaultSelected(tpe: ColumnType.Scalar): Option[SelectedType] = tpe match {
    case ColumnType.String =>
      SelectedType(TypeIndex(VARCHAR.ordinal), List(∃(Actual.integer(4096)))).some
    case ColumnType.Number =>
      SelectedType(TypeIndex(DECIMAL.ordinal), List(∃(Actual.integer(21)), ∃(Actual.integer(6)))).some
    case _ =>
      None
  }

  val render = RedshiftDestinationModule.RedshiftRenderConfig

  // If we ever wanted to make transactor initialized by queries we could modify `xa` to be `Resource[F, Transactor[F]]`
  def flowR(args: Flow.Args): Resource[F, Flow[F]] =
    RedshiftFlow(deleteService, put, config, xa.pure[Resource[F, *]], args)

  def destinationType: DestinationType =
    RedshiftDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F, RedshiftType]] =
    flowSinks
}

object RedshiftDestination {
  val OrdinalMap: Map[Int, RedshiftTypeId] =
    RedshiftTypeId.allIds
      .toList
      .map(id => (id.ordinal, id))
      .toMap
}
