package cn.flink.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
/**
  * 从网络端口中读取数据
  */
object DataSource_Socket {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = streamEnv.socketTextStream("192.168.3.142", 9999)

    val wordCount = dataStream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5),Time.seconds(2))
      .sum(1)



    wordCount.print()

    streamEnv.execute("DataSource_Socket")
  }
}
