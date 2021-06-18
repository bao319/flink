package cn.flink.sort

import cn.flink.table.Student
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

object SortTest {

  def main(args: Array[String]): Unit = {

    val batchEnv = ExecutionEnvironment.getExecutionEnvironment

    //stu(age,name,height)
    val mingxingDataSet: DataSet[(Int, String, Double)] = batchEnv.fromElements(
      (19, "xuzheng", 178.8),
      (17, "huangbo", 168.8),
      (19, "wangbaoqiang1", 174.8),
      (18, "wangbaoqiang3", 174.8),
      (17, "wangbaoqiang2", 174.8),
      (18, "liujing", 195.8),
      (18, "liutao", 182.7),
      (21, "huangxiaoming", 184.8)
    )

    val filePath: String = "c:/student.txt"
    val studentDataSet: DataSet[Student] = batchEnv.readCsvFile[Student](
      filePath = filePath,
      lineDelimiter = "\n",
      fieldDelimiter = ",",
      quoteCharacter = null,
      ignoreFirstLine = true,
      ignoreComments = "#",
      lenient = false,
      includedFields = Array[Int](0, 1, 2, 3, 4),
      pojoFields = Array[String]("id", "name", "sex", "age", "department")
    )



//    val result = mingxingDataSet.sortPartition(2, Order.DESCENDING).setParallelism(1)


//    val result = mingxingDataSet.sortPartition(2, Order.DESCENDING)
//      .sortPartition(0, Order.ASCENDING)
//    result.print()


    // flink怎么实现全局排序？
    val result = studentDataSet.partitionByRange("id").setParallelism(3)
      .sortPartition("id", Order.ASCENDING)


    result.writeAsText("C:/flink_sort1903_02", writeMode = WriteMode.OVERWRITE)


//    result.print()


    batchEnv.execute("sort")
  }
}
