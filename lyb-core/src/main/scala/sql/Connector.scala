package sql

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.descriptors._
import sql.EnvDemo.bsTableEnv

object Connector {
  def main(args: Array[String]): Unit = {
    // es
   bsTableEnv.connect(new Elasticsearch()
     .version("6")
     .host("localhost", 9200, "http")
     .index("person")
     .documentType("type")
   )
     .inUpsertMode()
     .withFormat(new Json())
     .withSchema(new Schema()
       .field("id", DataTypes.BIGINT())
       .field("name", DataTypes.STRING())
     )
     .createTemporaryTable("esTest")
    bsTableEnv.from("esTest").printSchema()

    //jdbc
    val sinkDDL =
      """
        |CREATE TABLE MyUserTable (
        |  id BIGINT,
        |  name STRING,
        |  age INT,
        |  status BOOLEAN,
        |  PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://localhost:3306/mydatabase',
        |   'table-name' = 'users',
        |   'username' = 'user',
        |   'password' = 'passwd'
        |)
        |""".stripMargin
    bsTableEnv.sqlUpdate(sinkDDL)
    bsTableEnv.from("MyUserTable").printSchema()
  }
}
