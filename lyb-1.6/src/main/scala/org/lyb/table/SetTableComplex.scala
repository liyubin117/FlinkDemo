package org.lyb.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row
import org.lyb.table.SetTable.Person

object SetTableComplex extends App{
  case class Address(id:Integer, address:String)

  //配置环境
  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
  //指定输入
  val person: DataSet[String] = env.fromElements(
    "1,liyubin,26",
    "2,li,27",
    "2,li,28",
    "3,yu,25",
    "4,bin,26"
  )
  val address: DataSet[String] = env.fromElements(
    "1,HZ",
    "2,XA",
    "3,YC"
  )
  //生成DataSet
  val personSet: DataSet[Person] = person.map(_.split(",")).map(x => Person(x(0).toInt, x(1), x(2).toInt))
  val addressSet = address.map(_.split(",")).map(x => Address(x(0).toInt, x(1)))

  //指定表程序
  val personT = tableEnv.fromDataSet(personSet,'id,'name,'age)
  val addressT = tableEnv.fromDataSet(addressSet,'id as 'aid,'address)
  val counts = personT
    .join(addressT)
    .where('id === 'aid) //三个等号
    .where('age >= 26)
    .select('id,'age)
    .groupBy('age)
    .select('age,'id.count)
    .orderBy('age.desc)
    .limit(0,2)


//  tableEnv.registerDataSet("person", personSet)
//  tableEnv.registerDataSet("address", addressSet)
//  val counts: Table = tableEnv.sqlQuery("select a.age,count(distinct a.id) from person a join address b on a.id=b.id where a.age>=26 group by a.age order by a.age desc limit 2")

  //结果转化为DataSet
  val result: DataSet[Row] = tableEnv.toDataSet[Row](counts)
  result.print()
}