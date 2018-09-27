package Table

import java.util.Date

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row
import org.joda.time.DateTime
import org.apache.flink.table.sources.DefinedRowtimeAttributes

object StreamTable extends App{

  //TODO: 定义一个实现DefinedRowtimeAttribute特质的类
//  class UserActionSource extends StreamTableSource[Row] with DefinedRowtimeAttribute {
//
//    override def getReturnType = {
//      val names = Array[String]("Username" , "Data", "UserActionTime")
//      val types = Array[TypeInformation[_]](Types.STRING, Types.STRING, Types.LONG)
//      Types.ROW(names, types)
//    }
//
//    override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
//      // create stream
//      // ...
//      // assign watermarks based on the "UserActionTime" attribute
//      val stream = inputStream.assignTimestampsAndWatermarks(...)
//      stream
//    }
//
//    override def getRowtimeAttribute = {
//      // Mark the "UserActionTime" attribute as event-time attribute.
//      "UserActionTime"
//    }
//  }

  //注册datastream
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tableEnv = TableEnvironment.getTableEnvironment(env)

  val host="spark"
  val port=9888
  val input = env.socketTextStream(host, port)
  val stream: DataStream[TimeTable] = input.map(_.split(",")).filter(_.length==2).map(x => TimeTable(x(0).asInstanceOf[DateTime], x(1)))

//  val counts = tableEnv.fromDataStream(person,'id,'name,'age).filter("age>=26").groupBy("age").select("age,id.count() as cnt")

  tableEnv.registerDataStream("TimeTable",stream)
//  val counts: Table = tableEnv.sqlQuery("select age,count(id) as cnt from person where age>=26 group by age")
  //TODO: 还存在问题 Window can only be defined over a time attribute column.
  // https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming.html#time-attributes
  val counts: Table = tableEnv.sqlQuery(
    "select id,count(id) as cnt,TUMBLE_END(timecol, INTERVAL '5' SECOND) as endT " +
      "from TimeTable where id>=26 " +
      "group by id,TUMBLE(timecol, INTERVAL '5' SECOND)"
  )

  //查看schema、执行计划
  counts.printSchema()
  println(tableEnv.explain(counts))

  val result: DataStream[(Boolean, Result)] = tableEnv.toRetractStream[Result](counts)
  result.print()
  env.execute()
}

case class TimeTable(timecol:DateTime, id:String)



