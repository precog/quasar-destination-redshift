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

import quasar.blobstore.s3.{AccessKey, Bucket, Region, SecretKey}

import argonaut._, Argonaut._

import java.net.URI

import org.specs2.mutable.Specification

object RedshiftConfigSpec extends Specification {
  val jdbcUri = "jdbc:redshift://redshift-cluster-1.example.outer-space.redshift.amazonaws.com:5439/dev"

  val woSchema =
    Json.obj(
      "bucket" := Json.obj(
        "bucket" := "foobarbucket",
        "accessKey" := "access key",
        "secretKey" := "secret key",
        "region" := "outer-space"),
      "jdbcUri" := jdbcUri,
      "user" := "awsuser",
      "password" := "super secret password")

  val common =
    woSchema.withObject(_ + ("schema", "my-schema".asJson))

  val roleAuth =
    Json.obj(
      "type":= "role",
      "arn":= "arn:aws:iam::account-id:role/test-role",
      "region":= "outer-space")

  val keyAuth =
    Json.obj(
      "type":= "keys",
      "accessKey":= "access key",
      "secretKey":= "secret key",
      "region":= "outer-space")

  "sanitizes config" >> {
    "with role authorization" >> {
      val sanitized =
        RedshiftDestinationModule.sanitizeDestinationConfig(
          common.withObject(_ + ("authorization", roleAuth)))

      val expected =
        Json.obj(
          "bucket":= Json.obj(
            "bucket":= "foobarbucket",
            "accessKey":= "access key",
            "secretKey":= "<REDACTED>",
            "region":= "outer-space"),
          "schema" := "my-schema",
          "jdbcUri":= jdbcUri,
          "user":= "awsuser",
          "password":= "<REDACTED>",
          "authorization" := roleAuth)

      sanitized must_== expected
    }

    "with key authorization" >> {
      val sanitized =
        RedshiftDestinationModule.sanitizeDestinationConfig(
          common.withObject(_ + ("authorization", keyAuth)))

      val expected =
        Json.obj(
          "bucket":= Json.obj(
            "bucket":= "foobarbucket",
            "accessKey":= "access key",
            "secretKey":= "<REDACTED>",
            "region":= "outer-space"),
          "schema" := "my-schema",
          "jdbcUri":= jdbcUri,
          "user":= "awsuser",
          "password":= "<REDACTED>",
          "authorization" := keyAuth.withObject(_ + ("secretKey", Json.jString("<REDACTED>"))))

      sanitized must_== expected
    }
  }

  "parses a valid config" >> {
    "with role authorization" >> {
      val testConfig =
        common.withObject(_ + ("authorization", roleAuth))

      testConfig.as[RedshiftConfig].result must beRight(
        RedshiftConfig(
          BucketConfig(
            Bucket("foobarbucket"),
            AccessKey("access key"),
            SecretKey("secret key"),
            Region("outer-space")),
          new URI(jdbcUri),
          User("awsuser"),
          Password("super secret password"),
          Authorization.RoleARN(
            "arn:aws:iam::account-id:role/test-role",
            Region("outer-space")),
          Some("my-schema")))
    }

    "with key authorization" >> {
      val testConfig =
        common.withObject(_ + ("authorization", keyAuth))

      testConfig.as[RedshiftConfig].result must beRight(
        RedshiftConfig(
          BucketConfig(
            Bucket("foobarbucket"),
            AccessKey("access key"),
            SecretKey("secret key"),
            Region("outer-space")),
          new URI(jdbcUri),
          User("awsuser"),
          Password("super secret password"),
          Authorization.Keys(
            AccessKey("access key"),
            SecretKey("secret key"),
            Region("outer-space")),
          Some("my-schema")))
    }
    "without schema" >> {
      val testConfig =
        woSchema.withObject(_ + ("authorization", roleAuth))

      testConfig.as[RedshiftConfig].result must beRight(
        RedshiftConfig(
          BucketConfig(
            Bucket("foobarbucket"),
            AccessKey("access key"),
            SecretKey("secret key"),
            Region("outer-space")),
          new URI(jdbcUri),
          User("awsuser"),
          Password("super secret password"),
          Authorization.RoleARN(
            "arn:aws:iam::account-id:role/test-role",
            Region("outer-space")),
          None))
    }
  }
}
