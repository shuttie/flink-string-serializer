package me.dfdx.flink

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInput, DataOutput}
import java.util.concurrent.TimeUnit

import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.types.StringValue
import org.openjdk.jmh.annotations._


@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
class StringDeserializerBenchmark {

  @Param(Array("ascii", "ascii-long", "utf1", "utf2", "utf3", "emoji", "random"))
  var stringType: String = _

  var item: String = _
  var jdkInBuf: ByteArrayInputStream = _
  var jdkInStream: DataInput = _

  var defaultInBuf: ByteArrayInputStream = _
  var defaultInStream: DataInput = _
  @Setup
  def setup = {
    item = StringGen.makeString(stringType)
    // jdk setup
    val jdkBuf = new ByteArrayOutputStream(128)
    val jdkStream = new DataOutputViewStreamWrapper(jdkBuf)
    jdkStream.writeUTF(item)
    jdkInBuf = new ByteArrayInputStream(jdkBuf.toByteArray)
    jdkInStream = new DataInputViewStreamWrapper(jdkInBuf)

    // default setup
    val defaultBuf = new ByteArrayOutputStream(128)
    val defaultStream = new DataOutputViewStreamWrapper(defaultBuf)
    StringValue.writeString(item, defaultStream)
    defaultInBuf = new ByteArrayInputStream(defaultBuf.toByteArray)
    defaultInStream = new DataInputViewStreamWrapper(defaultInBuf)
  }

  @Benchmark
  def deserializeJDK = {
    jdkInBuf.reset()
    jdkInStream.readUTF()
  }

  @Benchmark
  def deserializeDefault = {
    defaultInBuf.reset()
    StringValue.readString(defaultInStream)
  }

  @Benchmark
  def deserializeImproved = {
    defaultInBuf.reset()
    StringUtils.readString(defaultInStream)
  }

}
