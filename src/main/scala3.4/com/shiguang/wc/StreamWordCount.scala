package com.shiguang.wc

//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
// 流处理word count
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    设置并行度
//    env.setParallelism(8)

    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = paramTool.get("host")
    val port: Int = paramTool.getInt("port")

    //    接收一个socket文本流
    val inputDataStream: DataStream[String] = env.socketTextStream(host,port)

    val resultDataStream = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print().setParallelism(1)

//    启动任务执行
    env.execute("流处理word count")
  }
}
