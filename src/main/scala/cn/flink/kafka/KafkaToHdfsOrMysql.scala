package cn.flink.kafka

import cn.flink.sink.student
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
 * 功能描述：
 *
 * @Author: bao
 * @Date: 2021-06-03 14:00
 */
object KafkaConsumer {
  def main(args: Array[String]): Unit = {

//    val params: ParameterTool = ParameterTool.fromArgs(args)
//    val runType:String = params.get("runtype")
//    println("runType: " + runType)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.0.90.181:9092,10.0.90.182:9092,10.0.90.183:9092")
    properties.setProperty("group.id", "kafka_consumer")

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
      .setStartFromEarliest()  // 指定从最新offset开始消费

    val streamKafka = env.addSource(dataSource)
      .flatMap(_.toLowerCase.split(" "))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    val path = "hdfs://192.168.3.138:/kafka/word.txt"

    //hdfs
//    val sink = new BucketingSink[(String,Int)](path)
//    sink.setWriter(new StringWriter()).setBatchSize(20).setBatchRolloverInterval(2000)
    val dataStream = streamKafka.map(x=>student(x._1,x._2))

    //转table
    val tableEnv = StreamTableEnvironment.create(env)

    val table1: Table = tableEnv.fromDataStream(dataStream)

    table1.printSchema()

    //mysql
    dataStream.addSink(new MySink)

    // execute program
    env.execute("Flink Streaming—————KafkaSource")

  }
}
class MySink extends RichSinkFunction[student] {

  var stat: PreparedStatement = _
  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    buildPreparedStatement()
  }

  override def invoke(value: student): Unit = {
    stat.setString(1,value.name)
    stat.setInt(2,value.age)
    stat.executeLargeUpdate()
    stat.execute()
  }

  def buildPreparedStatement() {

    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://192.168.3.141:3306/nsfc", "root", "bigdata");
    stat = conn.prepareStatement("insert into student (name, age) values (?, ?)");

  }

  override def close(): Unit = {
    super.close()

    if(stat != null) stat.close()
    if(conn != null) conn.close()
  }

}
case class student(name: String, age: Int)

