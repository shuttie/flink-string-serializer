package me.dfdx.flink

import java.io.{ByteArrayOutputStream, DataOutput}

import org.apache.flink.core.memory.DataOutputViewStreamWrapper
import org.apache.flink.types.StringValue
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.scalacheck.Checkers

import scala.util.Random

class SerializerTest extends FlatSpec with Matchers with Checkers {
  val stringGenerator = Gen.choose(1, 100).map(len => Random.nextString(len))
  implicit val arbString = Arbitrary(stringGenerator)

  it should "encode ascii" in {
    check { (str: String) => {
      val original = encode(StringValue.writeString(str, _))
      val modified = encode(StringUtils.writeString(str, _))
      original.toList shouldBe modified.toList
    }}
  }


  def encode(f: (DataOutput) => Unit) = {
    val buf = new ByteArrayOutputStream(128)
    val stream = new DataOutputViewStreamWrapper(buf)
    f(stream)
    buf.toByteArray
  }
}
