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

import slamdata.Predef.{Eq => _, _}

import cats.Eq
import cats.data.Ior

import doobie.Fragment

import quasar.api.Labeled
import quasar.api.push.param._
import quasar.connector.destination.Constructor

sealed trait RedshiftTypeId {
  def ordinal: Int
}
sealed abstract class RedshiftType(spec: String) extends Product with Serializable {
  def id: RedshiftTypeId
  def asSql: Fragment = Fragment.const(spec)
}

object RedshiftTypeId {
  import RedshiftType._

  sealed abstract class SelfIdentified(spec: String, val ordinal: Int)
      extends RedshiftType(spec) with RedshiftTypeId {
    def id = this
  }

  sealed abstract class HigherKinded(val ordinal: Int) extends RedshiftTypeId {
    def constructor: Constructor[RedshiftType]
  }

  def allIds: Set[RedshiftTypeId] = Set(
    SMALLINT,
    INTEGER,
    BIGINT,
    FLOAT4,
    FLOAT,
    BOOL,
    DATE,
    TIME,
    TIMETZ,
    TIMESTAMP,
    TIMESTAMPTZ,
    TEXT,
    BPCHAR,
    DECIMAL,
    CHAR,
    VARCHAR)

  implicit val eq: Eq[RedshiftTypeId] = Eq.fromUniversalEquals
}

object RedshiftType {
  import RedshiftTypeId._

  // Self-identified
  case object SMALLINT extends SelfIdentified("SMALLINT", 0)
  case object INTEGER extends SelfIdentified("INTEGER", 1)
  case object BIGINT extends SelfIdentified("BIGINT", 2)
  case object FLOAT4 extends SelfIdentified("FLOAT4", 3)
  case object FLOAT extends SelfIdentified("FLOAT", 4)
  case object BOOL extends SelfIdentified("BOOL", 5)
  case object DATE extends SelfIdentified("DATE", 6)
  case object TIME extends SelfIdentified("TIME", 7)
  case object TIMETZ extends SelfIdentified("TIMETZ", 8)
  case object TIMESTAMP extends SelfIdentified("TIMESTAMP", 9)
  case object TIMESTAMPTZ extends SelfIdentified("TIMESTAMPTZ", 10)
  case object TEXT extends SelfIdentified("TEXT", 11)
  case object BPCHAR extends SelfIdentified("BPCHAR", 12)

  // Constructors
  case object DECIMAL extends HigherKinded(13) {
    def constructor = Constructor.Binary(PrecisionParam, ScaleParam, DECIMAL(_, _))
  }
  final case class DECIMAL(precision: Int, scale: Int) extends RedshiftType(s"DECIMAL($precision, $scale)") {
    def id = DECIMAL
  }

  case object CHAR extends HigherKinded(14) {
    def constructor = Constructor.Unary(CharLengthParam, CHAR(_))
  }
  final case class CHAR(length: Int) extends RedshiftType(s"CHAR($length)") {
    def id = CHAR
  }

  case object VARCHAR extends HigherKinded(15) {
    def constructor = Constructor.Unary(VarcharLengthParam, VARCHAR(_))
  }
  final case class VARCHAR(length: Int) extends RedshiftType(s"VARCHAR($length)") {
    def id = VARCHAR
  }

  // Params
  private val PrecisionParam: Labeled[Formal[Int]] =
    Labeled("Decimal precision", Formal.integer(Some(Ior.both(1, 38)), None, None))
  private val ScaleParam: Labeled[Formal[Int]] =
    Labeled("Decimal scale", Formal.integer(Some(Ior.both(1, 37)), None, None))
  private val CharLengthParam: Labeled[Formal[Int]] =
    Labeled("Length", Formal.integer(Some(Ior.both(1, 4096)), None, None))
  private val VarcharLengthParam: Labeled[Formal[Int]] =
    Labeled("Length", Formal.integer(Some(Ior.both(1, 65535)), None, None))


  implicit val eq: Eq[RedshiftType] = Eq.fromUniversalEquals
}
