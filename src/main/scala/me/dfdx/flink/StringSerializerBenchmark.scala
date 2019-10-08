package me.dfdx.flink

import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit

import org.apache.flink.core.memory.{DataOutputView, DataOutputViewStreamWrapper}
import org.apache.flink.types.StringValue
import org.openjdk.jmh.annotations._

import scala.util.Random


@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
class StringSerializerBenchmark {

  var buf: ByteArrayOutputStream = _
  var stream: DataOutputView = _
  var item: String = _
  val chars = "qwertyuiopasdfghklzxcvbnm".toCharArray
  @Setup(Level.Iteration)
  def setup = {
    buf = new ByteArrayOutputStream(32)
    stream = new DataOutputViewStreamWrapper(buf)
    item = (0 to 16).map(_ => chars(Random.nextInt(chars.length))).mkString("")
  }

  @Benchmark
  def measureOld = {
    buf.reset()
    StringValue.writeString(item, stream)
    buf.toByteArray
  }

  @Benchmark
  def measureNew = {
    buf.reset()
    StringSerializerImpl.writeString(item, stream)
    buf.toByteArray
  }

  @Benchmark
  def measureJDK = {
    buf.reset()
    stream.writeUTF(item)
    buf.toByteArray
  }
}
