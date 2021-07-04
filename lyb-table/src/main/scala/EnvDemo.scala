import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment}

object EnvDemo {

  //  def main(args: Array[String]): Unit = {
  //老版本 planner
  //流处理
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val settings = EnvironmentSettings.newInstance()
    .useOldPlanner()
    .inStreamingMode()
    .build()
  val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)
  //批处理
  val batchEnv = ExecutionEnvironment.getExecutionEnvironment
  val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

  // blink planner
  // 流处理
  val bsSettings = EnvironmentSettings.newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()
  val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)

  // 批处理
  val bbSettings = EnvironmentSettings.newInstance()
    .useBlinkPlanner()
    .inBatchMode()
    .build()
  //    val bbTableEnv = TableEnvironment.create(bbSettings)
  //  }

}
