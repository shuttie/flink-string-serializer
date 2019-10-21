package me.dfdx.flink

import java.io.{ByteArrayOutputStream, DataOutput}

import org.apache.flink.core.memory.DataOutputViewStreamWrapper

object Encoded {
  def apply(f: (DataOutput) => Unit) = {
    val buf = new ByteArrayOutputStream(128)
    val stream = new DataOutputViewStreamWrapper(buf)
    f(stream)
    buf.toByteArray
  }

}
