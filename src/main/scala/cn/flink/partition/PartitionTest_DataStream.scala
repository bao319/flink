package cn.flink.partition

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object PartitionTest_DataStream {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = streamEnv.socketTextStream("hadoop02", 4567)


    // 分区

    // 数据流进行分区，就只有自定义。么？
//    dataStream.partitionCustom()


    // round-robin 的均匀分区
    dataStream.rebalance

    /**
      * Spark中的算子：coalesce()   (只能减少，不能增加)
      * 如果需要过滤一个RDD中的数据，那么最佳实践：
      *   rdd.filter().coalesce().map()
      */
    dataStream.rescale  // （能增加分区，也能减少）

    // uniformly randomly 随机分区
    dataStream.shuffle

    // 广播
    dataStream.broadcast

  }
}
