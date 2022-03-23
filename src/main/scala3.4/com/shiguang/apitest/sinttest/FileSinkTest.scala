package com.shiguang.apitest.sinttest

import com.shiguang.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object FileSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile("src/main/resources/sensor.txt")

    //    先转换成样例类类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    dataStream.print()
    //    dataStream.writeAsCsv("src/main/resources/out.txt")
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("src/main/resources/out1.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )

    env.execute("file sink test")
  }
}
