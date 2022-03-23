package com.shiguang.apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

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
//      .window(TumblingEventTimeWindows.of(Time.seconds(15))) // 滚动时间窗口
//      .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(3)))  //滑动时间窗口，窗口时间
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10))) // 会话时间窗口
//      .timeWindow(Time.seconds(15)) // 时间窗口
//      .countWindow(10) //滚动窗口
      .countWindow(10,3) //滑动窗口

    env.execute("window test")
  }
}
