package com.shiguang.apitest.sinttest

import com.shiguang.apitest.{MySensorSource, SensorReading}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import java.sql
import java.sql.{Connection, DriverManager}


object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile("src/main/resources/sensor.txt")

    val stream = env.addSource(new MySensorSource())

    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    stream.addSink(new MyJdbcSinkFunc)

    env.execute("jdbc sink test")
  }
}

class MyJdbcSinkFunc() extends RichSinkFunction[SensorReading] {
  //  定义连接、预编译语句
  var conn: Connection = _
  var insertStmt: sql.PreparedStatement = _
  var updateStmt: sql.PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temp) values (?,?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //    先执行更新操作，查到就更新
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    //    如果更新没有查到数据，那么就插入
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}