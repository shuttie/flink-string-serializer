name := "flink-string-serializer-benchmark"

version := "0.1"

scalaVersion := "2.12.10"

lazy val flinkVersion = "1.9.0"

libraryDependencies ++= Seq(
  "org.apache.flink"          %% "flink-scala"                % flinkVersion,
  "org.apache.flink"          %% "flink-streaming-scala"      % flinkVersion,
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.scalacheck" %% "scalacheck" % "1.14.2" % "test"
)

enablePlugins(JmhPlugin)