package org.lyb.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.junit.Test
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

import scala.collection.mutable


class UDTFdemo {

  @Test
  def testLateralTVF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val userData = new mutable.MutableList[(String)]
    userData.+=(("Sunny#8"))
    userData.+=(("Kevin#36"))
    userData.+=(("Panpan#36"))

    val users = env.fromCollection(userData).toTable(tEnv, 'data)

    println(users.getSchema)

    val tvf = new SplitTVF()
    tEnv.registerTable("userTab", users)
    tEnv.registerFunction("splitTVF", tvf)

//    val sqlQuery = "SELECT data, name, age FROM userTab, LATERAL TABLE(splitTVF(data)) AS T(name, age)"

    val sqlQuery = "select data from userTab"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()


  }

}

// 定义一个简单的UDTF返回类型，对应接口上的 T
case class SimpleUser(name: String, age: Int)
// 继承TableFunction，并实现evale方法
// 核心功能是解析以#分割的字符串
class SplitTVF extends TableFunction[SimpleUser] {
  // make sure input element's format is "<string>#<int>"
  def eval(user: String): Unit = {
    if (user.contains("#")) {
      val splits = user.split("#")
      collect(SimpleUser(splits(0), splits(1).toInt))
    }
  }
}
