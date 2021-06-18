package cn.flink.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.sources.{CsvTableSource, TableSource}
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
//import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._

/**
  * Table API 入门程序
  *
  *   读取student.txt的数据，然后构架成一个Table对象，进行DSL和SQL操作
  *
  *   Table:
  *   name: student
  *   columns: id,name,sex,age,deparmtent
  *
  *   1、统计每个部门有多少人
  *     select department, count(*) as total from student group by department;
  *   2、统计部门人数超过5个的部门有那些？
  *     select department, count(*) as total from student group by department having total > 5
  */
object TableAPI_First {

  def main(args: Array[String]): Unit = {

    // 编程入口  TableEnvironment
//    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
////    val tableEnv: TableEnvironment = TableEnvironment.getTableEnvironment(batchEnv)
//    val tableEnv: TableEnvironment = BatchTableEnvironment.create(batchEnv)

    val settings = EnvironmentSettings.newInstance.inStreamingMode.build

    val tableEnv = TableEnvironment.create(settings)
    // 三种方式构建 Table对象 ：   1:Table，   2:DataSet   3:TableSource

//    val csvTableSource:CsvTableSource = new CsvTableSource(
//      path = "c:/student.txt",
//      fieldNames = Array[String]("id", "name", "sex", "age", "department"),
//      fieldTypes = Array[TypeInformation[_]](Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING),
//      fieldDelim = ",",
//      rowDelim = "\n",
//      ignoreFirstLine = true,
//      ignoreComments = "#"
//    )
    val csvTableSource: CsvTableSource = CsvTableSource.builder()
      .path("c:/student.txt")
      .lineDelimiter("\n")
      .fieldDelimiter(",")
      .ignoreFirstLine()
      .commentPrefix("#")
      .field("id", Types.INT)
      .field("name", Types.STRING)
      .field("sex", Types.STRING)
      .field("age", Types.INT)
      .field("department", Types.STRING)
      .build()
    // 仅仅只是注册。没有表
    val csvTable:Table = tableEnv.fromValues(csvTableSource)

    tableEnv.createTemporaryView("student",csvTable)

    // 拿到Table对象
    val studentTable: Table = tableEnv.scan("student")
//      .as("a1", "a2", "a3", "a4", "a5")


    // 处理结果
    // 输出表结构
    studentTable.printSchema()

    println("----------------------------------------------")
    // 输出表数据
    val studentDataSet: DataSet[Row] = studentTable.toDataSet[Row]
    studentDataSet.print()

    tableEnv.execute("table")
  }
}
