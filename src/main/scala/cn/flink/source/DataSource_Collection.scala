package cn.flink.source

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object DataSource_Collection {

  def main(args: Array[String]): Unit = {


    val batchEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取本地集合
    batchEnv.fromElements(1L, 2D, 3F, 4, 5, 6, 3, 3, 2, 2, 4, 4, 6, 6, 7).print()
    val dataset2 = batchEnv.fromElements[String]("a", "b", "c","d","d","c","a")

    batchEnv.execute(this.getClass.getSimpleName)
    //   val wordCount = dataset2.flatMap(_.split(",")).map(x =>(x,1)).groupBy(0).sum(1)
////    wordCount.setParallelism(2)
//    wordCount.writeAsText("D:\\project\\flink\\src\\main\\resources\\wordcount3.txt")
//    wordCount.print()
//    batchEnv.execute("wordcount")


//    val dataset3: DataSet[Int] = batchEnv.fromCollection(3 :: 4 :: 5 :: 6 :: Nil)
//    val dataset4: DataSet[String] = batchEnv.fromCollection(Array[String]("a", "b", "c"))
//    dataset3.print()
//
//    val dataset5: DataSet[Long] = batchEnv.generateSequence(1, 10)
//    dataset5.printToErr()
  }
}
