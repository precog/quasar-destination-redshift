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

import scala._
import scala.Predef._
import scala.concurrent.ExecutionContext
import scala.util.Random

import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.{Destination, DestinationError, DestinationType}
import quasar.blobstore.s3.{AccessKey, Region, S3DeleteService, S3PutService, SecretKey}
import quasar.connector.{DestinationModule, MonadResourceErr}
import quasar.concurrent.NamedDaemonThreadFactory

import argonaut._, Argonaut._

import cats.effect.{ConcurrentEffect, Concurrent, ContextShift, Resource, Sync, Timer}
import cats.data.EitherT
import cats.implicits._

import doobie.Transactor
import doobie.free.connection.isValid
import doobie.hikari.HikariTransactor
import doobie.implicits._

import eu.timepit.refined.auto._

import java.util.concurrent.Executors

import scalaz.NonEmptyList

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.{Region => AwsRegion}
import software.amazon.awssdk.services.s3.S3AsyncClient


object RedshiftDestinationModule extends DestinationModule {
  val RedshiftDriverFqcn = "com.amazon.redshift.jdbc.Driver"
  val Redacted = "<REDACTED>"
  val PoolSize = 10
  val PartSize = 10 * 1024 * 1024
  val LivenessTimeout = 60

  def destinationType = DestinationType("redshift", 1L)

  def sanitizeDestinationConfig(cfg: Json): Json =
    cfg.as[RedshiftConfig].toOption.fold(Json.jEmptyObject)(rsc =>
      rsc.copy(
        password = Password(Redacted),
        authorization = rsc.authorization match {
          case Authorization.RoleARN(_) =>
            rsc.authorization
          case Authorization.Keys(ak, _) =>
            Authorization.Keys(ak, SecretKey(Redacted))
        },
        uploadBucket = rsc.uploadBucket.copy(secretKey = SecretKey(Redacted))).asJson)

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
    config: Json): Resource[F, Either[InitializationError[Json], Destination[F]]] =
    (for {
      cfg <- EitherT.fromEither[Resource[F, ?]](config.as[RedshiftConfig].result) leftMap {
        case (err, _) => DestinationError.malformedConfiguration((destinationType, config, err))
      }

      jdbcUri = cfg.connectionUri.toString

      xa <- EitherT.right(for {
        poolSuffix <- Resource.liftF(Sync[F].delay(Random.alphanumeric.take(5).mkString))
        connectPool <- boundedPool[F](s"redshift-dest-connect-$poolSuffix", PoolSize)
        transactPool <- unboundedPool[F](s"redshift-dest-transact-$poolSuffix")
        transactor <- HikariTransactor.newHikariTransactor[F](
          RedshiftDriverFqcn,
          jdbcUri,
          cfg.user.value,
          cfg.password.value,
          connectPool,
          transactPool)
      } yield transactor)

      uploadCfg = cfg.uploadBucket

      client <- EitherT.right[InitializationError[Json]][Resource[F, ?], S3AsyncClient](
        s3Client[F](uploadCfg.accessKey, uploadCfg.secretKey, uploadCfg.region))

      _ <- EitherT(Resource.liftF(validConnection(xa, config)))

      deleteService = S3DeleteService(client, uploadCfg.bucket)

      putService = S3PutService(client, PartSize, uploadCfg.bucket)

      dest: Destination[F] = new RedshiftDestination[F](deleteService, putService, cfg, xa)

    } yield dest).value

  private def validConnection[F[_]: Sync](xa: Transactor[F], config: Json)
      : F[Either[InitializationError[Json], Unit]] =
    isValid(LivenessTimeout).transact(xa).map(valid =>
      if (valid)
        ().asRight
      else
        DestinationError
          .invalidConfiguration((destinationType, config, NonEmptyList("Couldn't connect to Redshift")))
          .asLeft)

  private def boundedPool[F[_]: Sync](name: String, threadCount: Int): Resource[F, ExecutionContext] =
    Resource.make(
      Sync[F].delay(
        Executors.newFixedThreadPool(
          threadCount,
          NamedDaemonThreadFactory(name))))(es => Sync[F].delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor(_))

  private def unboundedPool[F[_]: Sync](name: String): Resource[F, ExecutionContext] =
    Resource.make(
      Sync[F].delay(
        Executors.newCachedThreadPool(
          NamedDaemonThreadFactory(name))))(es => Sync[F].delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor(_))

  private def s3Client[F[_]: Concurrent](
    accessKey: AccessKey,
    secretKey: SecretKey,
    region: Region)
      : Resource[F, S3AsyncClient] = {
    val client =
      Concurrent[F].delay(
        S3AsyncClient.builder
          .credentialsProvider(
            StaticCredentialsProvider.create(
              AwsBasicCredentials.create(accessKey.value, secretKey.value)))
          .region(AwsRegion.of(region.value))
          .build)

    Resource.fromAutoCloseable[F, S3AsyncClient](client)
  }
}
