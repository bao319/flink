//package cn.flink.table
//
//import org.apache.flink.api.scala.{ExecutionEnvironment}
//import org.apache.flink.table.api.{Table, TableEnvironment, Types}
//import org.apache.flink.table.sources.CsvTableSource
//import org.apache.flink.table.api.bridge.scala._
//
//import org.apache.flink.api.scala._
//import org.apache.flink.table.api._
//
///**
//  * 注册得到Table的方式有三种：
//  *
//  * 1、注册tABLE
//  */
//object TableAPI_TableSource {
//
//  def main(args: Array[String]): Unit = {
//
////    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
//////    val tableEnv = TableEnvironment.getTableEnvironment(batchEnv)
////val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)
//
//    val settings = EnvironmentSettings.newInstance.inStreamingMode.build
//
//    val tableEnv = TableEnvironment.create(settings)
//
//    // 建造者设计模式
//    val csvTableSource: CsvTableSource = CsvTableSource.builder()
//      .path("c:/student.txt")
//      .lineDelimiter("\n")
//      .fieldDelimiter(",")
//      .ignoreFirstLine()
//      .commentPrefix("#")
//      .field("id", Types.INT)
//      .field("name", Types.STRING)
//      .field("sex", Types.STRING)
//      .field("age", Types.INT)
//      .field("department", Types.STRING)
//      .build()
//
//
//    tableEnv.registerTableSource("student", csvTableSource)
//    val studentTable: Table = tableEnv.scan("student")
//
//    studentTable.printSchema()
//    val dataset: DataSet[(Int, String, String, Int, String)] = studentTable.toDataSet[(Int, String, String, Int, String)]
//    dataset.print()
//
//
////    batchEnv.execute("table2")
//  }
//}
