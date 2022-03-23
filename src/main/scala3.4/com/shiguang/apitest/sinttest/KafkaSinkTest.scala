package com.shiguang.apitest.sinttest

import com.shiguang.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

import java.util.Properties

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    val inputStream = env.readTextFile("src/main/resources/sensor.txt")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.80.3:9092")
    properties.setProperty("group.id", "consumer-group")
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//    stream.print()
    val dataStream = inputStream
      .map( data=> {
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble).toString
      })

    dataStream.addSink( new FlinkKafkaProducer011[String]("192.168.80.3:9092","sinktest",  new SimpleStringSchema()))

    env.execute("Kafka sink test")
  }
}
