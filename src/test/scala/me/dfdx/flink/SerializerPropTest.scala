package me.dfdx.flink

import java.io.{ByteArrayOutputStream, DataOutput}

import org.apache.flink.core.memory.DataOutputViewStreamWrapper
import org.apache.flink.types.StringValue
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers, PropSpec}
import org.scalatestplus.scalacheck.{Checkers, ScalaCheckPropertyChecks}

import scala.util.Random

class SerializerPropTest extends PropSpec with Matchers with ScalaCheckPropertyChecks {
  val stringGenerator = Gen.choose(1, 100).map(len => Random.nextString(len))
  implicit val arbString = Arbitrary(stringGenerator)

  property("encode ascii") {
    forAll { (str: String) => {
      val original = Encoded(StringValue.writeString(str, _))
      val modified = Encoded(StringUtils.writeString(str, _))
      original.toList shouldBe modified.toList
    }}
  }

}