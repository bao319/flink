
package cn.flink.accumulator

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

object AccumulatorTest {

  def main(args: Array[String]): Unit = {

    val batchEnv = ExecutionEnvironment.getExecutionEnvironment

    // 请帮我统计一下  studnet.txt中，到底有多少行注释
    val filepath = "c:/student.txt"
    val lineDataSet: DataSet[String] = batchEnv.readTextFile(filepath)

    // 全局计数器  全局变量  这个变量是生存在Driver程序中
    var totalCount:Long = 0

    lineDataSet.map(new MapFunction[String, String] {
      override def map(value: String): String = {
        val flag: Boolean = value.startsWith("#")
        if (flag) {
          totalCount += 1
        }
        value
      }
    })
    print(totalCount)

  }
}
