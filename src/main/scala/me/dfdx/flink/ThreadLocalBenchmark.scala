package me.dfdx.flink

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import scala.util.Random


@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
class ThreadLocalBenchmark {

  var buf: ThreadLocal[String] = _
  var field: String = _

  @Setup
  def setup = {
    field = "none"
    buf = new ThreadLocal[String] {
      override def initialValue(): String = "none"
    }
  }

  @Benchmark
  def measureGet = {
    buf.get()
  }

  @Benchmark
  def measureField = {
    field
  }
}
