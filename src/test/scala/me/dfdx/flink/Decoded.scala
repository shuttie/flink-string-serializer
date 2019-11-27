package me.dfdx.flink

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInput, DataOutput}

import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}

object Decoded {
  def apply(bytes: Array[Byte], f: DataInput => String) = {
    val buf = new ByteArrayInputStream(bytes)
    val stream = new DataInputViewStreamWrapper(buf)
    f(stream)
  }
}
