package sql
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import sql.EnvDemo.{bsTableEnv, env}
object TableDemo {
  def main(args: Array[String]): Unit = {
    val filePath = "file/person.csv"
    bsTableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv) //定义文件格式
      .withSchema(new Schema()  //定义字段
        .field("id", DataTypes.BIGINT())
        .field("name", DataTypes.STRING())
        .field("age", DataTypes.BIGINT())
      )
      .createTemporaryTable("person")
    val personTable = bsTableEnv.from("person")
    val personStream = personTable.toAppendStream[(Long, String, Long)]
    personStream.print()

    env.execute("Table Demo")
  }
}
