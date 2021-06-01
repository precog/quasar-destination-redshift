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

import scala.Predef._
import scala._

import quasar.blobstore.s3.{
  AccessKey,
  Bucket,
  Region,
  SecretKey
}

import cats.implicits._

import argonaut._, Argonaut._

import java.net.{URI, URISyntaxException}

final case class User(value: String)
final case class Password(value: String)

sealed trait Authorization

object Authorization {
  final case class RoleARN(value: String, region: Region) extends Authorization
  final case class Keys(accessKey: AccessKey, secretKey: SecretKey, region: Region) extends Authorization
}

final case class BucketConfig(
  bucket: Bucket,
  accessKey: AccessKey,
  secretKey: SecretKey,
  region: Region)

final case class RedshiftConfig(
  uploadBucket: BucketConfig,
  connectionUri: URI,
  user: User,
  password: Password,
  authorization: Authorization,
  schema: Option[String])

object RedshiftConfig {
  private implicit val uriCodecJson: CodecJson[URI] =
    CodecJson(
      uri => Json.jString(uri.toString),
      c => for {
        uriStr <- c.jdecode[String]
        uri0 = Either.catchOnly[URISyntaxException](new URI(uriStr))
        uri <- uri0.fold(
          ex => DecodeResult.fail(s"Invalid URI: ${ex.getMessage}", c.history),
          DecodeResult.ok(_))
      } yield uri)

  private implicit val bucketConfigCodecJson: CodecJson[BucketConfig] =
    casecodec4[String, String, String, String, BucketConfig](
      (bucket, accessKey, secretKey, region) =>
        BucketConfig(Bucket(bucket), AccessKey(accessKey), SecretKey(secretKey), Region(region)),
      bc => (bc.bucket.value, bc.accessKey.value, bc.secretKey.value, bc.region.value).some)(
        "bucket", "accessKey", "secretKey", "region")

  private implicit val authorizationCodecJson: CodecJson[Authorization] =
    CodecJson[Authorization]({
      case Authorization.RoleARN(arn, region) =>
        Json.obj(
          "type" := "role",
          "arn" := arn,
          "region" := region.value)
      case Authorization.Keys(accessKey, secretKey, region) =>
        Json.obj(
          "type" := "keys",
          "accessKey" := accessKey.value,
          "secretKey" := secretKey.value,
          "region" := region.value)
    }, (c => for {
      authType <- c.get[String]("type")
      res <- authType match {
        case "role" => for {
          arn <- c.get[String]("arn")
          region <- c.get[String]("region")
        } yield Authorization.RoleARN(arn, Region(region))
        case "keys" =>
          for {
            accessKey <- c.get[String]("accessKey")
            secretKey <- c.get[String]("secretKey")
            region <- c.get[String]("region")
          } yield Authorization.Keys(AccessKey(accessKey), SecretKey(secretKey), Region(region))
        case _ => DecodeResult.fail("'auth' must be 'role' or 'keys'", c.history)
      }
    } yield res))

  implicit val redshiftConfigCodecJson: CodecJson[RedshiftConfig] =
    casecodec6[BucketConfig, URI, String, String, Authorization, Option[String], RedshiftConfig](
      (bucketConfig, uri, user, password, auth, schema) =>
        RedshiftConfig(bucketConfig, uri, User(user), Password(password), auth, schema),
      rsc =>
        (rsc.uploadBucket,
          rsc.connectionUri,
          rsc.user.value,
          rsc.password.value,
          rsc.authorization,
          rsc.schema).some)(
      "bucket", "jdbcUri", "user", "password", "authorization", "schema")
}
