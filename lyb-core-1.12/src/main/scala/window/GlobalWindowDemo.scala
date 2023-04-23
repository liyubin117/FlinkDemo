package window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.scala._

object GlobalWindowDemo extends App{
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val text: DataStream[String] = env.fromElements(
    "To be, or not to be,--that is the question:--",
    "Whether 'tis nobler in the mind to suffer",
    "The slings and arrows of outrageous fortune",
    "Or to take arms against a sea of troubles,"
  )
  val cnt = text.flatMap(_.split("\\W"))
    .map(x => WordWithCount(x,1))
    .keyBy("word")
    .window(GlobalWindows.create)
//    .trigger(new GlobalWindows.NeverTrigger)
    .sum("count")

  cnt.print().setParallelism(2)
  env.execute("FuncDemo")

  case class WordWithCount(word:String, count:Int)

}
