package cn.flink.sink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

object SinkTest_Print {

  def main(args: Array[String]): Unit = {

    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


    val dataSet1: DataSet[Int] = batchEnv.fromElements(1,2,3,4,5)


    // 底层使用：  System.out.println()
    dataSet1.print()
    // 底层使用：  System.err.println()
    dataSet1.printToErr()
    // 上面这两个方式，都表示，要把所有TaskManager的数据拉取到客户端。


    // 那些节点那些结果数据，当前那些节点就自己直接打印输出，而不用传输给Client
    dataSet1.printOnTaskManager("abc")
  }
}
