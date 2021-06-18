package cn.flink.partition

import cn.flink.table.Student
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

/**
  * 数据分区：
  * 1、随机分区
  * 2、轮询分区
  * 3、Hash分区
  * 4、范围分区
  * 5、自定义分区
  */
object PartitionTest_Batch {

  def main(args: Array[String]): Unit = {

    val batchEnv = ExecutionEnvironment.getExecutionEnvironment

    val filePath: String = "c:/student.txt"
    val dataset: DataSet[Student] = batchEnv.readCsvFile[Student](
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

    /**
      * MapReduce中默认的 Hash规则：
      * 1、如果遇到字符串，就计算该字符串的 hash值
      * 2、如果遇到数值类型，则当前这个数值，就是Hash值
      */
    var ptn_number = 7
    //    val ptnResult = dataset.partitionByHash("department").setParallelism(ptn_number)
    //    val ptnResult = dataset.partitionByRange("id").setParallelism(ptn_number)
//    val ptnResult = dataset.partitionCustom(new MyPartitioner(), "sex").setParallelism(ptn_number)

    // 均匀分区
    val ptnResult = dataset.rebalance().setParallelism(ptn_number)




    ptnResult.writeAsText("c:/ptn_test1903_08")
    batchEnv.execute("ptn_test1903")
  }
}

class MyPartitioner extends Partitioner[String] {

  // key: 你传给我的值
  // numPartitoins ： 分区个数
  // Int： 分区编号
  // partition方法： 分区规则
  override def partition(key: String, numPartitions: Int): Int = {
    //   if(key.isInstanceOf[String]){
    //      val newkey = key.asInstanceOf[String]
    //
    //    }else if(key.isInstanceOf[Int]){
    //  }

    (key.hashCode & Integer.MAX_VALUE) % numPartitions
  }
}
