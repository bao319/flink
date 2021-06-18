package cn.flink.sink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import scala.collection.JavaConversions._

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

object SinkTest_My {

  def main(args: Array[String]): Unit = {

    val StreamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    StreamEnv.setParallelism(1)

    val list = new util.ArrayList[student]()
    list.add(student("aa",12))
    list.add(student("bb",12))
    list.add(student("bb",12))
    list.add(student("we",12))
    list.add(student("uu",12))


    StreamEnv.fromCollection(list)
      .addSink(new MySink())
      .disableChaining()

    StreamEnv.execute("MySink")
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