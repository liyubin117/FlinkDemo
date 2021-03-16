package sql

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import sql.EnvDemo.bsTableEnv
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import sql.function.{Avg, HashCode, Split}
import sql.EnvDemo.env

object FunctionDemo {
  def main(args: Array[String]): Unit = {
    env.setParallelism(1)

    val filePath = "file/person.csv"
    val schema = new Schema() //定义字段
      .field("id", DataTypes.BIGINT())
      .field("name", DataTypes.STRING())
      .field("age", DataTypes.BIGINT())

    bsTableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv) //定义文件格式
      .withSchema(schema)
      .createTemporaryTable("person")

    // Table api方式
    val hash = new HashCode(2.1)
    bsTableEnv.from("person")
      .select('id, 'name, hash('name) as 'hashcode)
      .toAppendStream[Row]
      .print("scalar function by tableApi")
    val split = new Split(":")
    bsTableEnv.from("person")
      .joinLateral(split('name) as ('col1,'col2) )
      .toAppendStream[Row]
      .print("table function by tableApi")
    val avg = new Avg
    bsTableEnv.from("person")
        .groupBy('id)
        .aggregate(avg('age) as 'col)
        .select('id, 'col)
        .toRetractStream[Row]
        .print("aggregate function by tableApi")

    // Sql api方式
    bsTableEnv.registerFunction("hash", new HashCode(2.1))
    bsTableEnv.sqlQuery("select id,name,hash(name) as hashcode from person")
      .toAppendStream[Row]
      .print("scalar function by sqlApi")
    bsTableEnv.registerFunction("split", new Split(":"))
    bsTableEnv.sqlQuery(
      """
        |select id,name,age,word,length
        |from person,
        |lateral table(split(name)) as splitid(word, length)
      """.stripMargin)
        .toAppendStream[Row]
        .print("table function by sqlApi")

    env.execute()

  }
}

