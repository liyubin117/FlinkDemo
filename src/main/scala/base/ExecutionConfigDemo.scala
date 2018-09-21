package base

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object ExecutionConfigDemo extends App{
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  //检查点
  env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE, true)  //启用检查点
  env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
  env.getCheckpointConfig.setCheckpointInterval(5000)
  //重启策略
  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.of(5, TimeUnit.SECONDS))) //尝试重启3次，间隔5秒
  println(env.getRestartStrategy.getDescription)
  //后端状态
  env.setStateBackend(new MemoryStateBackend(10, false))

  val config = env.getConfig


  val input = env.generateSequence(1,10)

}
