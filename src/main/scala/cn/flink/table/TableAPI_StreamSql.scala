package cn.flink.table


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}

object TableAPI_StreamSql extends Serializable {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv :StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)

    val dataStream: DataStream[String] = streamEnv.socketTextStream("192.168.3.142", 9999)

    val value: DataStream[person] = dataStream.map(x => {
      val id = x.split(" ")(0)
      val name = x.split(" ")(1)
      val age = x.split(" ")(2)
      person(id, name, age)
    })

    tableEnv.createTemporaryView("stu", value, $"id2",$"name2",$"age2")

    //    table.printSchema()
//    val table: Table = tableEnv.sqlQuery("select * from stu")


    //    tableEnv.createTemporaryView("stu2",table)
    val result: TableResult = tableEnv.executeSql("select name2,count(*) c from stu group by name2 having count(*) >1")

    //    val wordCount =  table.groupBy("f0").select('f0, 'f0.count as 'count)

    result.print()
    streamEnv.execute("TableAPI_StreamSql")

  }
}
case class person(id:String,name:String,age:String)