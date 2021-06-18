package cn.flink.table

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * 注册得到Table的方式有三种：
 *
 * 3、 从已有的TABLE构建TABLE
 */
object TableAPI_Table {

  def main(args: Array[String]): Unit = {

    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    //    val tableEnv = TableEnvironment.getTableEnvironment(batchEnv)
    val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)


    val studentADataSet: DataSet[StudentB] = batchEnv.readCsvFile[StudentB](filePath = "c:/student.txt", ignoreFirstLine = true, ignoreComments = "#",
      includedFields = Array[Int](0, 1, 2, 3, 4),
      pojoFields = Array[String]("id", "name", "sex", "age", "department")
    )


    tableEnv.registerDataSet[StudentB]("student", studentADataSet)
    val studentTable: Table = tableEnv.scan("student")


    val newTable: Table = studentTable.filter($"age" > 18)
      .select($"id,name,age")
      .as("myid, myname,myage")


    newTable.printSchema()
    val newResultDataSet: DataSet[(Int, String, Int)] = tableEnv.toDataSet[(Int, String, Int)](newTable)
    newResultDataSet.print()


  }
}

case class StudentB(id: Int, name: String, sex: String, age: Int, department: String)