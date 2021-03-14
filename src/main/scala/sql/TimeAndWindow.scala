package sql

import sql.EnvDemo.{bsTableEnv, env}
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.descriptors.{Csv, Kafka, Rowtime, Schema}
import sql.TableDemo.schema

object TimeAndWindow {
  def main(args: Array[String]): Unit = {
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // DataStream -> Table时指定时间属性
    val stream = env.fromElements("1,li,1000","2,yubin,2000")
      .map(x => {
        val arr = x.split(",")
        (arr(0), arr(1), arr(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Long)](Time.seconds(10)) {
        override def extractTimestamp(t: (String, String, Long)): Long = t._3 * 1000
      })
    stream.print("print")
    // 定义一个额外的plug字段指定处理时间，ts字段指定事件时间
    val table = bsTableEnv.fromDataStream(stream, ${"id"}, ${"name"}, $("ts"), ${"eventime"}.rowtime(), ${"processtime"}.proctime())
    table.printSchema()

    // Table api指定时间属性
    bsTableEnv.connect(new Kafka()
      .version("0.11")
      .topic("test")
      .property("zookeeper.connect", "localhost:2181")
      .property("boostrap.server", "localhost:9092")
    ).withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.BIGINT())
        .field("name", DataTypes.STRING())
//        .field("processtime", DataTypes.TIMESTAMP(3)).proctime()
        .field("eventtime", DataTypes.TIMESTAMP(3)).rowtime(new Rowtime()
              .timestampsFromField("ts")
              .watermarksPeriodicBounded(1000))
      )
      .createTemporaryTable("personKafka")

    // sql api指定时间属性
    val ddl =
      """
        |CREATE TABLE user_actions (
        |  id BIGINT,
        |  name STRING,
        |  ts BIGINT,
        |  rt as TO_TIMESTAMP(FROM_UNIXTIME(ts)),
        |  WATERMARK FOR rt AS rt - INTERVAL '5' SECOND
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'user_behavior',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'csv'
        |)
        |""".stripMargin
    bsTableEnv.executeSql(ddl)
    print(bsTableEnv.from("user_actions").getSchema)

    env.execute("TimeAndWindow")
  }
}
