//package cn.flink.source
//
//import org.apache.flink.api.java.io.TextInputFormat
//import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
//import org.apache.flink.core.fs.Path
//
//object DataSource_File {
//
//  def main(args: Array[String]): Unit = {
//    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
//
//    val filePath:String = "d:/student.csv"
////    val dataset1: DataSet[String] = batchEnv.readTextFile(filePath)
////    dataset1.print()
////    println(dataset1.count())
//
////    val dataset2: DataSet[Student] = batchEnv.readCsvFile[Student](
////      filePath = filePath,
////      lineDelimiter = "\n",
////      fieldDelimiter = ",",
////      quoteCharacter = null,
////      ignoreFirstLine = true,
////      ignoreComments = "#",
////      lenient = false,
////      includedFields = Array[Int](0, 1, 2, 3, 4 ,5),
////      pojoFields = Array[String]("id", "name", "school", "department","subject","rank")
////    )
////    dataset2.print()
//
//    // 通用的读取文件的方法
//    val dataset3 = batchEnv.readFile(new TextInputFormat(new Path(filePath)), filePath)
//
//    dataset3.print()
//
//
//  }
//}
//
//case class Student(id:String, name:String, school:String,department:String,subject:String,rank:String)