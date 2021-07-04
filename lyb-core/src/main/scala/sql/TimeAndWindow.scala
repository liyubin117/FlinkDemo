package sql

import sql.EnvDemo.{bsTableEnv, env}
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.descriptors.{Csv, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row
import sql.TableDemo.schema

object TimeAndWindow {
  def main(args: Array[String]): Unit = {
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // DataStream -> Table时指定时间属性
    val stream = env.fromElements("1,li,1000"
      ,"2,yubin,2000"
      ,"3,three,3000"
      ,"2,two,4000")
      .map(x => {
        val arr = x.split(",")
        (arr(0), arr(1), arr(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, Long)](Time.seconds(10)) {
        override def extractTimestamp(t: (String, String, Long)): Long = t._3 * 1000
      })
    stream.print("print")
    // 定义一个额外的plug字段指定处理时间，ts字段指定事件时间
    val table = bsTableEnv.fromDataStream(stream, 'id, 'name, 'ts, 'eventime.rowtime(), 'processtime.proctime())
    table.printSchema()

    //Group Window
    val result = table
//      .window(Tumble over 3.minutes on 'eventtime as 'w)  // eventime字段3分钟的滚动事件时间窗口
      .window(Slide over 50.seconds every 30.seconds on 'eventime as 'w)
//      .window(Tumble over 3.rows on 'eventime as 'w)  //3行内的计数窗口
//      .window(Session withGap 10.minutes on 'eventime as 'w)  // 10分钟的会话窗口
      .groupBy('id, 'w)
      .select('id, 'id.count, 'w.start, 'w.end)
    bsTableEnv.toAppendStream[Row](result).print("Group Window result")

    //Over Window
    val overWindow = table
//      .window(Over partitionBy 'id orderBy 'eventime preceding UNBOUNDED_RANGE as 'w)  //无界
//      .window(Over partitionBy 'id orderBy 'eventime preceding UNBOUNDED_ROW as 'w)
//      .window(Over partitionBy 'id orderBy 'eventime preceding 1.minutes as 'w) //有界
      .window(Over partitionBy 'id orderBy 'eventime preceding 2.rows as 'w)
      .select('id, 'ts, 'id.max over 'w)
    bsTableEnv.toAppendStream[Row](overWindow).print("Over Window result")




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
