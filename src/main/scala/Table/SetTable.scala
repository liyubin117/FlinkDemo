package Table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}

object SetTable extends App{
  //配置环境
  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
  //指定输入
  val input: DataSet[String] = env.fromElements(
    "1,liyubin,26",
    "2,li,27",
    "3,yu,25",
    "4,bin,26",
    "4,bin,26"
  )
  //注册person表
  val personSet: DataSet[Person] = input.map(_.split(",")).map(x => Person(x(0).toInt, x(1), x(2).toInt))
  //指定表程序
  val counts: Table = tableEnv.fromDataSet(personSet).filter("age>=26").groupBy("age").select("age,id.count() as cnt")
  //结果转化为DataSet
  val result: DataSet[Result] = tableEnv.toDataSet[Result](counts)
  result.print()
}

case class Person(id:Int, name:String, age:Int)
case class Result(age:Int, cnt:Long)