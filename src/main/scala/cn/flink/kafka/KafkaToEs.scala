package cn.flink.kafka

import org.apache.flink.api.common.functions.{MapFunction, RuntimeContext}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}
import java.util.Properties
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


/**
 * 功能描述：
 *
 * @Author: bao
 * @Date: 2021-06-03 14:00
 */
object KafkaToEs {
  def main(args: Array[String]): Unit = {

//    val params: ParameterTool = ParameterTool.fromArgs(args)
//    val runType:String = params.get("runtype")
//    println("runType: " + runType)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.0.90.181:9092,10.0.90.182:9092,10.0.90.183:9092")
    properties.setProperty("group.id", "kafka_consumer")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // exactly-once 语义保证整个应用内端到端的数据一致性
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    // 开启检查点并指定检查点时间间隔为5s
//    env.enableCheckpointing(5000) // checkpoint every 5000 msecs
    // 设置StateBackend，并指定状态数据存储位置
//    env.setStateBackend(new FsStateBackend("file:///D:/Temp/checkpoint/flink/KafkaSource"))

    val KAFKA_TOPIC: String = "flink_kafka"

    val dataSource: FlinkKafkaConsumerBase[String] = new FlinkKafkaConsumer(
      KAFKA_TOPIC,
      new SimpleStringSchema(),
      properties)
//      .setStartFromEarliest()
      .setStartFromLatest()

    val streamKafka = env.addSource(dataSource)
//      .flatMap(_.toLowerCase.split(" "))
//      .map((_, 1))
//      .keyBy(0)
//      .timeWindow(Time.seconds(5))
//      .sum(1)
    streamKafka.print()
//    val sink = new BucketingSink[(String,Int)](path)
//    sink.setWriter(new StringWriter()).setBatchSize(20).setBatchRolloverInterval(2000)
//    val dataStream = streamKafka.map(x=>student(x._1,x._2))
//    dataStream.print()

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("192.168.3.140", 9200, "http"))
    httpHosts.add(new HttpHost("192.168.3.141", 9200, "http"))
    httpHosts.add(new HttpHost("192.168.3.142", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](httpHosts,
      new ElasticsearchSinkFunction[String] {
        def createIndexRequest(element: String): IndexRequest = {

          val map = new java.util.HashMap[String, String]
          map.put("name", element.split(" ")(0))
          map.put("gender", element.split(" ")(1))
          map.put("haah", element.split(" ")(2))

           Requests.indexRequest()
            .index("myflink").`type`("_doc")
            .source(map)
        }
        override def process(element: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(element))

        }
      }
    )

    esSinkBuilder.setBulkFlushMaxActions(1)

    streamKafka.addSink(esSinkBuilder.build)

    env.execute("flink_es")

  }



}
