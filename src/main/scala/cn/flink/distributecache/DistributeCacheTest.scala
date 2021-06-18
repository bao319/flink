package cn.flink.distributecache

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

import scala.collection.mutable

object DistributeCacheTest {

  def main(args: Array[String]): Unit = {

    // 大表数据  id,pv
    val biglist = List("1,11", "2,22","1,33", "2,44","1,55", "3,66")

    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 你把文件设置到某个地方，必须要让这个应用程序的所有 任务节点，都能访问这个文件。
    batchEnv.registerCachedFile("hdfs://myha01/flink1903/dc/input/flink_dc1903.txt", "dcfile");

    val bigDataSet = batchEnv.fromCollection(biglist);


    val joinResultDataSet: DataSet[String] = bigDataSet.map(new RichMapFunction[String, String] {

      // 存储小表的所有数据
      val idPvMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()


      // 读取分布式缓存的文件，把数据加载到内存中：idPvMap
      override def open(parameters: Configuration): Unit = {

        // 获取缓存的文件到本地
        val dcFile: File = getRuntimeContext.getDistributedCache.getFile("dcfile")
        val br: BufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(dcFile)))

        // 首先读取一行
        var line: String = br.readLine()
        while (line != null) {
          // 解析
          val fields: Array[String] = line.split(",")
          val id = fields(0)
          val name = fields(1)
          idPvMap.put(id, name)

          // 再读一行
          line = br.readLine();
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
    })


    joinResultDataSet.print()
  }
}
