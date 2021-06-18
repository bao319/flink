package cn.flink.broadcast

import java.util

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

import scala.collection.mutable

/**
  * list1 和 bigdata 在driver中
  *
  */
object BroadCastTest {

  def main(args: Array[String]): Unit = {
    // 小表数据   id,name
    val list1 = List("1,huangbo", "2,xuzheng", "3,wangbaoqiang")

    // 大表数据  id,pv
    val biglist = List("1,11", "2,22","1,33", "2,44","1,55", "3,66")


    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


    val toBCDataSet: DataSet[String] = batchEnv.fromCollection(list1)
    val bigDataSet = batchEnv.fromCollection(biglist);

//    bigDataSet.join(dataset1)
    // select id,name,pv from a join b on a.id = b.id;

    /**
      * 如果上面的直接内连接，产生数据倾斜， 那么就使用 BroadCast
      *
      * 泛型String的具体数据格式：1,huangbo,11
      */
    val joinResultDataSet: DataSet[String] = bigDataSet.map(new RichMapFunction[String, String] {

      // 存储小表的所有数据
      val idPvMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()

      // 初始化（open方法一定会在所有的map方法执行之前，先执行一次，）
      // 用来拿广播数据集
      override def open(parameters: Configuration): Unit = {

        val smallTable: util.List[String] = getRuntimeContext().getBroadcastVariable("smallTable")
        val iter = smallTable.iterator()
        while (iter.hasNext()) {
          // id_pv_str      1,huangbo
          val id_pv_str: String = iter.next()
          val fields: Array[String] = id_pv_str.toString.split(",")
          idPvMap.put(fields(0), fields(1))
        }
      }

      override def map(value: String): String = {

        // value:  1,11
        val id: String = value.toString.split(",")(0)
        val pv: String = value.toString.split(",")(1)
        val name: String = idPvMap.getOrElse(id, "")

        // 1,huangbo,11
        id + "," + name + "," + pv
      }

      // 做一个广播的操作
    }).withBroadcastSet(toBCDataSet, "smallTable")



    joinResultDataSet.print()


  }
}
