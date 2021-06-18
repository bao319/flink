
package cn.flink.accumulator

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object AccumulatorTest2 {

  def main(args: Array[String]): Unit = {

    val batchEnv = ExecutionEnvironment.getExecutionEnvironment

    // 请帮我统计一下  studnet.txt中，到底有多少行注释
    val filepath = "c:/student.txt"
    val lineDataSet: DataSet[String] = batchEnv.readTextFile(filepath)

    // 全局计数器  全局变量  这个变量是生存在Driver程序中
//    var totalCount:Long = 0

    val result = lineDataSet.map(new RichMapFunction[String, String] {

      var totalCount: Long = _

      override def open(parameters: Configuration): Unit = {
        totalCount = 0
      }

      override def map(value: String): String = {
        val flag: Boolean = value.startsWith("#")
        if (flag) {
          totalCount += 1
        }
        value
      }

      override def close(): Unit = {
        println(totalCount)
      }
    }).setParallelism(1)



    result.writeAsText("c:/xxxxx.txt", writeMode = WriteMode.OVERWRITE)
    batchEnv.execute("xxxxx")
  }
}
