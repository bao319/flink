package cn.flink.sink

import cn.flink.table.Student
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

object SinkTest_File {

  def main(args: Array[String]): Unit = {


    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment



    val filePath:String = "c:/student.txt"
    val dataset2: DataSet[Student] = batchEnv.readCsvFile[Student](
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



    // 省略  T


    // 执行SInk操作
    val outputpath = "c:/studnent_output_1903_01"
    dataset2.writeAsText(outputpath, writeMode = WriteMode.OVERWRITE)


    dataset2.writeAsCsv("c:/studnent_output_1903_02",
      rowDelimiter = "\n",
      fieldDelimiter = "\t",
      writeMode = WriteMode.OVERWRITE
    ).setParallelism(1)



    batchEnv.execute("SinkTest_File")
  }
}
