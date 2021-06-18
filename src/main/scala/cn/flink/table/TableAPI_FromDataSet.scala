package cn.flink.table

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.bridge.scala._

import org.apache.flink.api.scala._
import org.apache.flink.table.api._

/**
 * 注册得到Table的方式有三种：
 *
 * 2 、 从DataSet获取Table
 */
object TableAPI_FromDataSet {

  def main(args: Array[String]): Unit = {

    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    //    val tableEnv = TableEnvironment.getTableEnvironment(batchEnv)
    val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)


    val studentADataSet: DataSet[StudentA] = batchEnv.readCsvFile[StudentA](filePath = "c:/student.txt", ignoreFirstLine = true, ignoreComments = "#",
      includedFields = Array[Int](0, 1, 2, 3, 4),
      pojoFields = Array[String]("id", "name", "sex", "age", "department")
    )

    val studentInfo  = tableEnv.fromDataSet(studentADataSet)
    tableEnv.registerTable("student", studentInfo )
    val studentTable: Table = tableEnv.scan("student")


    studentTable.printSchema()
    val dataset: DataSet[(Int, String, String, Int, String)] = studentTable.toDataSet[(Int, String, String, Int, String)]
    dataset.print()
  }
}

case class StudentA(id: Int, name: String, sex: String, age: Int, department: String)