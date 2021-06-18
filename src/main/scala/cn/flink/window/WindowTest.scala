package cn.flink.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {

  def main(args: Array[String]): Unit = {


    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment


    val dataStream: DataStream[String] = streamEnv.socketTextStream("10.0.90.181", 9966)


    val wordDataStream = dataStream.flatMap(line => line.split(" "))
    val resultDataStream = wordDataStream.map(word => (word, 1))
      .keyBy(0)
//      .timeWindow(Time.seconds(6),Time.seconds(4))
        .countWindow(5, 2)
      .sum(1)

    resultDataStream.print()

    streamEnv.execute("WindowTest")
  }

}
