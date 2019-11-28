package me.dfdx.flink

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutput}

import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.types.StringValue
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers, PropSpec}
import org.scalatestplus.scalacheck.{Checkers, ScalaCheckPropertyChecks}

import scala.util.Random

class SerializerPropTest extends PropSpec with Matchers with ScalaCheckPropertyChecks {
  val stringGenerator = Gen.choose(1, 100).map(len => Random.nextString(len))
  implicit val arbString = Arbitrary(stringGenerator)
  implicit val arbInt = Arbitrary(Gen.chooseNum(2, 100))
  implicit val config = PropertyCheckConfiguration(minSuccessful = 1000000)

  property("ensure that original and new impl produce the same byte sequences") {
    forAll { (str: String) => {
      val original = Encoded(StringValue.writeString(str, _))
      val modified = Encoded(StringUtils.writeString(str, _))
      val br=1
      original.toList shouldBe modified.toList
    }}
  }

  property("ensure that new impl can read any byte sequence made by the original impl") {
    forAll { (str: String) => {
      val original = Encoded(StringValue.writeString(str, _))
      val modified = Encoded(StringUtils.writeString(str, _))
      original.toList shouldBe modified.toList
    }}
  }

  property("should roundtrip the same string") {
    forAll { (str: String) => {
      val original = Encoded(StringValue.writeString(str, _))
      val decoded = Decoded(original, StringUtils.readString)
      decoded shouldBe str
    }}
  }

  property("should roundtrip multiple strings over the same buffer") {
    forAll { (str: String, count: Int) => {
      val buf = new ByteArrayOutputStream(0)
      val stream = new DataOutputViewStreamWrapper(buf)
      for {
        _ <- 0 until count
      } {
        StringUtils.writeString(str, stream)
      }
      stream.close()

      val in = new ByteArrayInputStream(buf.toByteArray)
      val readStream = new DataInputViewStreamWrapper(in)
      val result = for {
        _ <- 0 until count
      } yield {
        StringUtils.readString(readStream)
      }
      result.toList shouldBe List.fill(count)(str)

    }}
  }

}
