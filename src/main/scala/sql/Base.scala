package sql

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{$, Table}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.types.Row

object Base {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    val ds: DataStream[(String, Int)] = env.fromElements("a", "a", "c", "a")
      .map(x => (x, 1))
      .keyBy(0)
      .reduce((x, y) => (x._1, x._2 + y._2))
    // DataStream -> Table
    val t:Table = tableEnv.fromDataStream(ds, $("key"), $("v"))

    // Table api
    val resultTable:Table = t
      .select("key, v")
      .filter("key == 'a'")
    resultTable.printSchema()
    //Table -> DataStream
    val resultDataStream = resultTable.toRetractStream[(String,Int)]
    resultDataStream.print()

    // SQL api
    tableEnv.createTemporaryView("t", resultTable)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select count(distinct v) as cnt
        |from t
        |""".stripMargin)
    val resultSqlDataStream = resultSqlTable.toRetractStream[Row]
    resultSqlDataStream.print()

    env.execute("flink sql base")

  }

}
