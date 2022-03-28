package com.shtd.contest.streaming

import java.util.Properties

import com.shtd.contest.etl.extractjob
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

case class Test(aa: Double)

object Flinkstream {

  val REDISIP = extractjob.RedisIP //redisip
  val ZKUrl = extractjob.XKIP + ":2181" //zk url+ip
  val KAFKAURL = extractjob.KafkaIP + ":9092" //kafka ip
  val TOPIC = "topictest"
  val GROUPID = "Test1"

  def main(args: Array[String]): Unit = {

    var env = StreamExecutionEnvironment.getExecutionEnvironment
    if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    }
    val properties = new Properties()
    properties.setProperty("zookeeper.connect", ZKUrl)
    properties.setProperty("bootstrap.servers", KAFKAURL)
    properties.setProperty("group.id", GROUPID)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(6)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val kafkaSource = new FlinkKafkaConsumer(TOPIC, new SimpleStringSchema, properties)


    val datastream: DataStream[Test] = env.addSource(kafkaSource).filter(x => {
      "正确值".equals(x.split(":")(0)) //过滤数据
    }).map(x => {
      //获取到想要的数据并转成 Test类，方便凑走
      Test(x.split(":")(1).split(",")(3).toDouble)
    })


    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost(REDISIP).build()


    //数据进行累加计算，在插入redis
    datastream.map(x => {
      ("aaTest", x.aa)
    }).keyBy(_._1).sum(1).map(x => (x._1, x._2.toString))
      .addSink(new RedisSink[(String, String)](config, new MyRedisMapper) {})


  }

}

class MyRedisMapper extends RedisMapper[(String, String)] {
  //  方法用于指定对接收来的数据进行什么操作
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }

  //  于指定接收到的数据中哪部分作为key
  override def getKeyFromData(data: (String, String)): String = {
    data._1
  }

  //  方法用于指定接收到的数据中哪部分作为value
  override def getValueFromData(data: (String, String)): String = {
    data._2
  }
}



