package me.dfdx.flink

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.types.StringValue
import org.scalatest.{FlatSpec, Inspectors, Matchers}

class SerializerTest extends FlatSpec with Matchers with Inspectors {
  val cases = Map(
    "ascii" -> "foo bar",
    "numbers" -> "12345",
    "1kb strings" -> (0 to 1000).map(_ => "a").mkString(""),
    "0x7F to 0xFF ascii" -> new String((127 to 250).map(_.toChar).toArray),
    "0x00 to 0xFF ascii" -> new String((0 to 250).map(_.toChar).toArray),
    "0xFF to 0x2FF ascii" -> new String((250 to 767).map(_.toChar).toArray),
    "0x0FFF to 0xFFFF ascii" -> new String((4095 to 32766).map(_.toChar).toArray),
  )

  it should "work with mixed strings" in {
    val str = new String(Array(32000.toChar, 32000.toChar, 32000.toChar))
    val original = Encoded(StringValue.writeString(str, _))
    val decoded = Decoded(original, StringUtils.readString)
    decoded shouldBe str
  }

  forAll(cases.toList) { pair => {
    val (name, str) = pair
    it should s"encode $name" in {
      val a = Encoded(StringValue.writeString(str, _)).toList
      val b = Encoded(StringUtils.writeString(str, _)).toList
      a shouldBe b
    }
  }}

  forAll(cases.toList) { pair => {
    val (name, str) = pair
    it should s"decode $name" in {
      val original = Encoded(StringValue.writeString(str, _))
      val decoded = Decoded(original, StringUtils.readString)
      decoded shouldBe str
    }
  }}

    forAll(cases.toList) { pair => {
      val (name, str) = pair
      it should s"encode multiple '$name' in the same buffer" in {
        val buf = new ByteArrayOutputStream(0)
        val stream = new DataOutputViewStreamWrapper(buf)
        for {
          _ <- 0 until 100
        } {
          StringUtils.writeString(str, stream)
        }
        stream.close()

        val in = new ByteArrayInputStream(buf.toByteArray)
        val readStream = new DataInputViewStreamWrapper(in)
        val result = for {
          _ <- 0 until 100
        } yield {
          StringUtils.readString(readStream)
        }
        result.toList shouldBe List.fill(100)(str)
      }
    }}
}
