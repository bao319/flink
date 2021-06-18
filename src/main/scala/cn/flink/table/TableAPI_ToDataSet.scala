package cn.flink.table

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.apache.flink.table.api.bridge.scala._

object TableAPI_ToDataSet {

  def main(args: Array[String]): Unit = {


    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
//    val tableEnv = TableEnvironment.getTableEnvironment(batchEnv)
val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)


    val studentADataSet: DataSet[StudentD] = batchEnv.readCsvFile[StudentD](filePath = "c:/student.txt", ignoreFirstLine = true, ignoreComments = "#",
      includedFields = Array[Int](0, 1, 2, 3, 4),
      pojoFields = Array[String]("id", "name", "sex", "age", "department")
    )


    tableEnv.registerDataSet[StudentD]("student", studentADataSet)
    val studentTable: Table = tableEnv.scan("student")


    /**
      * DataSet  DataFrame
      *
      * DataFrame = DataSet<Row>
      *   DataSet<Student>
      */


    // Row
    val ds1: DataSet[Row] = studentTable.toDataSet[Row]

    // StudentD  case class
    val ds2: DataSet[StudentD] = tableEnv.toDataSet[StudentD](studentTable)

    // 元祖
    val ds3: DataSet[(Int, String, String, Int, String)] = tableEnv.toDataSet[(Int, String, String, Int, String)](studentTable)

    // primitive type 原始类型
    val ds4: DataSet[Long] = studentTable.select("age").toDataSet[Long]

    // pojo对象
    val ds5: DataSet[StudentE] = tableEnv.toDataSet[StudentE](studentTable)
  }

}

case class StudentD(id:Int, name:String, sex:String, age:Int, department:String)

