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

import quasar.blobstore.s3.{AccessKey, Bucket, Region, SecretKey}

import argonaut._, Argonaut._

import java.net.URI

import org.specs2.mutable.Specification

object RedshiftConfigSpec extends Specification {
  "parses a valid config" >> {
    val common =
      Json.obj(
        "bucket":= Json.obj(
          "bucket":= "foobarbucket",
          "accessKey":= "access key",
          "secretKey":= "secret key",
          "region":= "outer-space"),
        "jdbcUri":= "jdbc:redshift://redshift-cluster-1.example.outer-space.redshift.amazonaws.com:5439/dev",
        "user":= "awsuser",
        "password":= "super secret password")

    "with role authorization" >> {
      val testConfig = common.withObject(c =>
        c + ("authorization", Json.obj(
             "type":= "role",
             "arn":= "arn:aws:iam::account-id:role/test-role",
             "region":= "outer-space")))

      testConfig.as[RedshiftConfig].result must beRight(
        RedshiftConfig(
          BucketConfig(
            Bucket("foobarbucket"),
            AccessKey("access key"),
            SecretKey("secret key"),
            Region("outer-space")),
          new URI("jdbc:redshift://redshift-cluster-1.example.outer-space.redshift.amazonaws.com:5439/dev"),
          User("awsuser"),
          Password("super secret password"),
          Authorization.RoleARN(
            "arn:aws:iam::account-id:role/test-role",
            Region("outer-space"))))
    }

    "with key authorization" >> {
      val testConfig = common.withObject(c =>
        c + ("authorization", Json.obj(
          "type":= "keys",
          "accessKey":= "access key",
          "secretKey":= "secret key",
          "region":= "outer-space")))

      testConfig.as[RedshiftConfig].result must beRight(
        RedshiftConfig(
          BucketConfig(
            Bucket("foobarbucket"),
            AccessKey("access key"),
            SecretKey("secret key"),
            Region("outer-space")),
          new URI("jdbc:redshift://redshift-cluster-1.example.outer-space.redshift.amazonaws.com:5439/dev"),
          User("awsuser"),
          Password("super secret password"),
          Authorization.Keys(
            AccessKey("access key"),
            SecretKey("secret key"),
            Region("outer-space"))))
    }
  }
}
