package sql
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import sql.EnvDemo.{bsTableEnv, env}
object TableDemo {
  def main(args: Array[String]): Unit = {
    //csv table
    val filePath = "file/person.csv"
    val schema = new Schema()  //定义字段
      .field("id", DataTypes.BIGINT())
      .field("name", DataTypes.STRING())
      .field("age", DataTypes.BIGINT())

    bsTableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv) //定义文件格式
      .withSchema(schema)
      .createTemporaryTable("person")
    val personTable = bsTableEnv.from("person")
          .filter($("age") > 20)
          .select($("id"), $("name"))
    val personStream = personTable.toAppendStream[(Long, String)]
    personStream.print("result")

    bsTableEnv.from("person")
      .filter($("age") > 25)
      .groupBy($("id"))
      .select($("id"), $("name").count.as("cnt"))
      .toRetractStream[(Long, Long)]
      .print("aggre result")

    //kafka table
    bsTableEnv.connect(new Kafka()
      .version("0.11")
      .topic("test")
      .property("zookeeper.connect", "localhost:2181")
      .property("boostrap.server", "localhost:9092")
    ).withFormat(new Csv())
      .withSchema(schema)
      .createTemporaryTable("personKafka")

    env.execute("Table Demo")
  }
}
