package com.shiguang.apitest

import org.apache.flink.streaming.api.scala._

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile("src/main/resources/sensor.txt")

    //    先转换成样例类类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val resultStream = dataStream
      .map(data => (data.id, data.temperature))
      //.keyBy(data => data._1)  // 按照二元组的第一个元素（id）分组
      .keyBy(_._1)
//      .window()

    env.execute("window test")
  }
}
