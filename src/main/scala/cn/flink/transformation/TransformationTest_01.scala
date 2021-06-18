package cn.flink.transformation

import cn.flink.sink.student
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

import java.util

object TransformationTest_01 {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStreamSource[String] = streamEnv.socketTextStream("hadoop02", 6666)

    /**
      *
      *   class A extends B
      *
      *   DataStream[A]  DataStream[B]
      *
      *   DataStream[String]    SingleOutputStreamOperator[String]
      */

    val list = new util.ArrayList[student]()
    list.add(student("aa",12))
    list.add(student("bb",12))
    list.add(student("bb",12))
    list.add(student("we",12))
    list.add(student("uu",12))

    val stu = streamEnv.fromCollection(list)

    val new_stu = stu.map(new MapFunction[student,student] {
      override def map(t: student): student = {
        val name = t.name
        val age = t.age + 1
        student(name,age)
      }
    })

    new_stu.print()
    streamEnv.execute("aaa")

  }
}
