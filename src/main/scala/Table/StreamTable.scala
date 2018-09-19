package Table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableEnvironment

object StreamTable extends App{
  //注册datastream
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tableEnv = TableEnvironment.getTableEnvironment(env)

  val host="spark"
  val port=9888
  val input = env.socketTextStream(host, port)
  val person: DataStream[Person] = input.map(_.split(",")).filter(_.length==3).map(x => Person(x(0).toInt, x(1), x(2).toInt))

//  val counts = tableEnv.fromDataStream(person).filter("age>=26").groupBy("age").select("age,id.count() as cnt")

  tableEnv.registerDataStream("person",person)
  val counts = tableEnv.sqlQuery("select age,count(id) as cnt from person where age>=26 group by age")

  counts.printSchema()
  val result = tableEnv.toRetractStream[Result](counts)
  result.print()
  env.execute()
}
