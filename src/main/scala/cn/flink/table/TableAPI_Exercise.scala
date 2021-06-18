package cn.flink.table

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row


/**
  * 读取student.txt的数据，然后构架成一个Table对象，进行DSL和SQL操作
  * Table:
  * name: student
  * columns: id,name,sex,age,deparmtent
  *
  * 1、统计每个部门有多少人
  * select department, count(*) as total from student group by department;
  * 2、统计部门人数超过5个的部门有那些？
  * select department, count(*) as total from student group by department having total > 5
  *
  * 发现问题：
  *   当使用SQL的时候， having分支，执行出错
  *
  *   标记：
  *   having 的执行在select的前面
  */
object TableAPI_Exercise {

  def main(args: Array[String]): Unit = {

    val batchEnv:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(batchEnv)


    val studentADataSet: DataSet[Student] = batchEnv.readCsvFile[Student](filePath = "d:/student.csv",
      ignoreFirstLine = true,
      ignoreComments = "#",
      includedFields = Array[Int](0, 1, 2, 3, 4,5),
      pojoFields = Array[String]("id", "name", "school", "department","subject","rank")
    )

    tableEnv.registerDataSet[Student]("student", studentADataSet)

    /**
      * 第一种方式：
      * 使用DSL风格
      */
//     val studentTable: Table = tableEnv.scan("student")
//     val resultTable: Table = studentTable.groupBy("subject")
//       .select("subject, id.count as totalcount")
//       .orderBy("totalcount.desc")
//     resultTable.printSchema()
//     val resultDataSet = tableEnv.toDataSet[Row](resultTable)
//     resultDataSet.print()

    /**
      * 第二种方式：
      * 使用SQL的方式
      */
    val sql =  """
        select subject, count(*) as c
        from student
        group by subject
        having count(*) > 6
        order by c desc
      """.stripMargin
    val resultTable1:Table = tableEnv.sqlQuery(sql)

    val resultDataSet1 = tableEnv.toDataSet[Row](resultTable1)
    resultDataSet1.print()


    /**
      * 第三：
      * 使用DSL风格来实现第二个需求：
      * 统计部门人数超过6个的部门有那些？
      */
//    val table1: Table = studentTable
//      .groupBy("department")
//      .select("department, id.count as totalcount")
//      .as("mydpt, count")
//      .where("count > 6")
//      .orderBy("count.desc")
//
//    table1.printSchema()
//    val table1DataSet: DataSet[(String, Long)] = tableEnv.toDataSet[(String, Long)](table1)
//    table1DataSet.print()
  }
}

case class Student(id:String, name:String, school:String,department:String,subject:String,rank:String)