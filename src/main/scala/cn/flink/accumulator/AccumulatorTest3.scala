
package cn.flink.accumulator

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object AccumulatorTest3 {

  def main(args: Array[String]): Unit = {

    val batchEnv = ExecutionEnvironment.getExecutionEnvironment

    // 请帮我统计一下  studnet.txt中，到底有多少行注释
    val filepath = "c:/student.txt"
    val lineDataSet: DataSet[String] = batchEnv.readTextFile(filepath)

    // 全局计数器  全局变量  这个变量是生存在Driver程序中
//    var totalCount:Long = 0

    val result = lineDataSet.map(new RichMapFunction[String, String] {

      // 存在于一个Task中的一个变量
      var counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {

        // 注册一个全局计数器
        getRuntimeContext.addAccumulator("counter", counter)
      }

      override def map(value: String): String = {
        val flag: Boolean = value.startsWith("#")
        if (flag) {
          counter.add(1)
        }
        value
      }

      override def close(): Unit = {
//        println(totalCount)
      }
    }).setParallelism(3)

    result.writeAsText("c:/xxxxx.txt", writeMode = WriteMode.OVERWRITE)


    val executeResult: JobExecutionResult = batchEnv.execute("xxxxx")

    val totalCount: Long = executeResult.getAccumulatorResult[Long]("counter")

    println(totalCount)
  }
}
