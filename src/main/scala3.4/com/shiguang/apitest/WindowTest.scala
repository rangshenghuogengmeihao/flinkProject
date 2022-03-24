package com.shiguang.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // val inputStream = env.readTextFile("src/main/resources/sensor.txt")
    val inputStream = env.socketTextStream("192.168.80.3", 7777)

    // 先转换成样例类类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 每十五秒统计一次，窗口内各传感器所有温度的最小值，以及最新的时间戳
    val resultStream = dataStream
      .map(data => (data.id, data.temperature, data.timestamp))
      //.keyBy(data => data._1)  // 按照二元组的第一个元素（id）分组
      .keyBy(_._1)
      // .window(TumblingEventTimeWindows.of(Time.seconds(15))) // 滚动时间窗口
      // .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(3)))  //滑动时间窗口，窗口时间
      // .window(EventTimeSessionWindows.withGap(Time.seconds(10))) // 会话时间窗口
      .timeWindow(Time.seconds(15)) // 滚动时间窗口
      // .timeWindow(Time.seconds(15),Time.seconds(5)) // 滑动时间窗口
      // .countWindow(10) //滚动计数窗口
      // .countWindow(10,3) //滑动窗口
      // .minBy("temperature")
      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))
    // .reduce(new MyReducer)

    resultStream.print()

    env.execute("window test")
  }
}

class MyReducer extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id, value2.timestamp, value1.temperature.min(value2.temperature))
  }
}