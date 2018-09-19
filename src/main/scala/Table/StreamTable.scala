package Table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}

object StreamTable extends App{
  //注册datastream
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tableEnv = TableEnvironment.getTableEnvironment(env)

  val host="spark"
  val port=9888
  val input = env.socketTextStream(host, port)
  val person: DataStream[Person] = input.map(_.split(",")).filter(_.length==3).map(x => Person(x(0).toInt, x(1), x(2).toInt))

  val counts = tableEnv.fromDataStream(person,'id,'name,'age).filter("age>=26").groupBy("age").select("age,id.count() as cnt")

//  tableEnv.registerDataStream("person",person)
//  val counts: Table = tableEnv.sqlQuery("select age,count(id) as cnt from person where age>=26 group by age")

  //查看schema、执行计划
  counts.printSchema()
  println(tableEnv.explain(counts))

  val result: DataStream[(Boolean, Result)] = tableEnv.toRetractStream[Result](counts)
  result.print()
  env.execute()
}
