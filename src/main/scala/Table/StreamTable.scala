//package Table
//
//import java.beans.Expression
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.TableEnvironment
//
//object StreamTable extends App{
//  //注册datastream
//  val env = StreamExecutionEnvironment.getExecutionEnvironment
//  val tableEnv = TableEnvironment.getTableEnvironment(env)
//
//  val host="spark"
//  val port=9888
//  val input = env.socketTextStream(host, port)
//  val person = input.map(_.split(",")).map(x => Person(x(0).toInt, x(1), x(2).toInt))
//  tableEnv.registerDataStream("person", person)
//}
//
//case class Person(id:Int, name:String, age:Int)