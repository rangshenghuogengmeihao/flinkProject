package com.shiguang.apitest.sinttest

import com.shiguang.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile("src/main/resources/sensor.txt")

    //    先转换成样例类类型
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("")
      .setPort(6379)
      .build()



    dataStream.addSink(
      new RedisSink[SensorReading]( conf,new MyRedisMapper)
    )

    env.execute("redis sink test")
  }
}

//    定义一个RedisMapper
class MyRedisMapper extends RedisMapper[SensorReading]{
//  定义保存数据写入Redis的命令,HSET 表明 key value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }

//  将温度值指定为value
  override def getKeyFromData(data: SensorReading): String = {
    data.temperature.toString
  }

//将id指定为key
  override def getValueFromData(data: SensorReading): String = {
      data.id
  }
}