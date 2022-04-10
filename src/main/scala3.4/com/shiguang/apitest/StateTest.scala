package com.shiguang.apitest

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

import java.util

object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("192.168.80.", 7777)
    inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })


    env.execute("state test")

  }
}

// Keyed state测试：必须定义在RichFunction中，因为需要运行时上下文
class MyRichMapper extends RichMapFunction[SensorReading, String] {
  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(
    new ListStateDescriptor[Int]("liststate",classOf[Int])
  )
  lazy val mapState: MapState[String,Double] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String,Double]("mapstate",classOf[String],classOf[Double])
  )
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(
    new ReducingStateDescriptor[SensorReading]("reducestate",new MyReducer,classOf[SensorReading])
  )

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valuestate", classOf[Double]))
  }

  override def map(value: SensorReading): String = {
    // 状态的读写
    val myV = valueState.value()
    valueState.update(value.temperature)
    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(new util.ArrayList())
    listState.update(list)
    listState.get()

    mapState.contains("sensor_1")
    mapState.get("sensor_1")
    mapState.put("sensor_1",1.3)

    reduceState.get()


    value.id
  }
}