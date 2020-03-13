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

package quasar.destination

import quasar.blobstore.s3.SecretKey

package object redshift {
  val Redacted = "<REDACTED>"

  def redactAuth(auth: Authorization): Authorization =
    auth match {
      case Authorization.RoleARN(_, _) =>
        auth
      case ak @ Authorization.Keys(_, _, _) =>
        ak.copy(secretKey = SecretKey(Redacted))
    }
}
