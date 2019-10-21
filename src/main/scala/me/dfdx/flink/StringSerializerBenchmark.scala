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
  @Param(Array("ascii", "ascii-long", "utf1", "utf2", "utf3", "emoji", "random"))
  var stringType: String = _

  var item: String = _
  @Setup(Level.Iteration)
  def setup = {
    buf = new ByteArrayOutputStream(128)
    stream = new DataOutputViewStreamWrapper(buf)
    item = StringGen.makeString(stringType)
  }

  @Benchmark
  def serializeDefault = {
    buf.reset()
    StringValue.writeString(item, stream)
    buf.toByteArray
  }

  @Benchmark
  def serializeImproved = {
    buf.reset()
    StringUtils.writeString(item, stream)
    buf.toByteArray
  }

  @Benchmark
  def serializeJDK = {
    buf.reset()
    stream.writeUTF(item)
    buf.toByteArray
  }
}
