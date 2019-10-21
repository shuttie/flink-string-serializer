package me.dfdx.flink

import org.apache.flink.types.StringValue
import org.scalatest.{FlatSpec, Inspectors, Matchers}

class SerializerTest extends FlatSpec with Matchers with Inspectors {
  val cases = List(
    "foo bar",
    "12345",
    (0 to 1000).map(_ => "a").mkString(""),
    new String((0 to 250).map(_.toChar).toArray)
  )

  forAll(cases) { str => {
    it should s"encode $str" in {
      val a = Encoded(StringValue.writeString("foo bar", _)).toList
      val b = Encoded(StringUtils.writeString("foo bar", _)).toList
      a shouldBe b
    }
  }}
}
