package cn.flink.source

import org.apache.flink.api.common.functions.FilterFunction

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}

object DataSource_My {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val studentDataStream: DataStreamSource[(String,String,String)] = streamEnv
      .addSource[(String,String,String)](new MySourceFunction())

    studentDataStream.setParallelism(7).print()

    streamEnv.execute("DataSource_My")
  }
}


/**
  * 这个类去负责读取数据的
  *
  *   例子：从mysql中读取数据
  *
  *   JDBC：读取MySQL数据
  *   五个步骤：
  *   1、注册驱动类
  *   2、获取连接
  *   3、通过连接对象，获取一个操作对象：Statement
  *       以上这三个步骤，不管在做沈阳的SQL语句执行，都是需要进行操作的，而且最好的方式，肯定不是执行一次，获取一个连接
  *       初始化
  *
  *   4、通过Statement操作对象，执行一个SQL语句
  *   5、解析结果
  *       以上两个步骤，根据需求执行丢应的SQL
  *
  *   6、close
  *       收尾操作
  *
  */

class MySourceFunction extends RichParallelSourceFunction[(String,String,String)]{

  var statement: PreparedStatement = _
  var conn: Connection = _

  // 初始化
  override def open(parameters: Configuration): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://192.168.3.141:3306/nsfc", "root", "bigdata")
    statement = conn.prepareStatement("select id,country,province from city")
  }

  // run方法
  override def run(ctx: SourceFunction.SourceContext[(String,String,String)]): Unit = {

    val resultSet: ResultSet = statement.executeQuery()
    while(resultSet.next()){
      val id:String = resultSet.getString("id")
      val country:String = resultSet.getString("country")
      val province:String = resultSet.getString("province")

      ctx.collect(id,country,province)
    }
  }
  // 收尾操作
  override def close(): Unit = {
    if(statement != null) statement.close()
    if(conn != null) conn.close()
  }
  override def cancel(): Unit = {
    close()
  }
}



case class city(id:Int, country:String, province:String)

//class Mapper {
//
//  void setup(context);
//
//  void map(key, value, context);
//
//  void cleanup(context);
//
//  void run(context){
//    // 初始化 一个mapTask只会调用一次
//    setup(context);
//    while(context.nextKeyValue()){
//      // 实际执行业务逻辑处理，会调用很多很多次
//      map(context.getCurrnetkey(), context.getCurrentValue(), context)
//    }
//    // 一个mapTask只会调用一次
//    cleanup(context);
//  }
//
//}


//  class MySinkFunction extends RichSinkFunction[MyStudent]{
//
//    override def open(parameters: Configuration): Unit = {
//
//    }
//
//    override def invoke(value: MyStudent, context: SinkFunction.Context[_]): Unit = {
//
//
//    }
//
//    override def close(): Unit = {
//
//
//    }
//}