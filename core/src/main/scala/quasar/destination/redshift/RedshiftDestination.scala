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

import quasar.api.ColumnType
import quasar.api.destination.DestinationType
import quasar.blobstore.services.{DeleteService, PutService}
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{LegacyDestination, ResultSink}

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import doobie._

final class RedshiftDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
  deleteService: DeleteService[F],
  put: PutService[F],
  config: RedshiftConfig,
  xa: Transactor[F]) extends LegacyDestination[F] with Flow.Sinks[F] {

  val render = RedshiftDestinationModule.RedshiftRenderConfig

  // If we ever wanted to make transactor initialized by queries we could modify `xa` to be `Resource[F, Transactor[F]]`
  def flowR(args: Flow.Args): Resource[F, Flow[F]] =
    RedshiftFlow(deleteService, put, config, xa.pure[Resource[F, *]], args)

  def destinationType: DestinationType =
    RedshiftDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F, ColumnType.Scalar]] =
    flowSinks
}
