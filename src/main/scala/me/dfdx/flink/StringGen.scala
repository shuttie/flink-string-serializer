package me.dfdx.flink

import scala.util.Random

object StringGen {
  def makeString(tpe: String) = tpe match {
    case "ascii" => "foobar"
    case "ascii-long" => (0 to 100).map(_ => "a").mkString("")
    case "utf1" => "мама мыла раму"
    case "utf2" => "転需細視調多一菱先耕乃下談若策和成験移"
    case "utf3" => "לוח וקשקש פולנית ממונרכיה אם."
    case "emoji" => "\uD83D\uDC98\uD83D\uDC34\uD83C\uDF63\uD83D\uDC33\uD83D\uDCCB\uD83D\uDCD3\uD83C\uDFB3"
    case "random" => Random.nextString(100)
  }
}
