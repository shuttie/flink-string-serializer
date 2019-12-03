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
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
class StringSerializerBenchmark {

  var buf: ByteArrayOutputStream = _
  var stream: DataOutputView = _
  @Param(Array("ascii"/*, "russian", "chinese"*/))
  var stringType: String = _

  //@Param(Array("1", "4", "8", "16", "32", "64", "128"))
  @Param(Array("1", "2", "3", "4", "5", "6", "7"))
  var length: String = _

  var item: String = _
  @Setup(Level.Iteration)
  def setup = {
    buf = new ByteArrayOutputStream(128)
    stream = new DataOutputViewStreamWrapper(buf)
    item = StringGen.fill(StringGen.symbolMap(stringType), length.toInt)
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

//  @Benchmark
//  def serializeJDK = {
//    buf.reset()
//    stream.writeUTF(item)
//    buf.toByteArray
//  }
}
