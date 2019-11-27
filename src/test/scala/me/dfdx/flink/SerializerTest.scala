package me.dfdx.flink

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
  )

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
}
