package me.dfdx.flink

import scala.util.Random

object StringGen {
  val symbolMap = Map(
    "ascii" -> "a",
    "russian" -> "ж",
    "chinese" -> "転"
  )
  def fill(symbol: String, len: Int) = (0 until len).map(_ => symbol).mkString("")
}
