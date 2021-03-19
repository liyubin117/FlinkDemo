package base

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.apache.flink.util.Collector

object IntervalJoinDemo extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  //创建黑色元素数据集
  val blackStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (4, 1L), (5, 4L))
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, Long)](Time.seconds(3)) {
      override def extractTimestamp(element: (Int, Long)): Long = element._2
    })
  //创建白色元素数据集
  val whiteStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (1, 1L), (3, 4L))
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, Long)](Time.seconds(3)) {
      override def extractTimestamp(element: (Int, Long)): Long = element._2
    })
  //通过Join方法将两个数据集进行关联
  val windowStream: DataStream[String] = blackStream.keyBy(_._1)
    //调用intervalJoin方法关联另外一个DataStream
    .intervalJoin(whiteStream.keyBy(_._1))
    //设定时间上限和下限
    .between(Time.milliseconds(-2), Time.milliseconds(1))
    .process(new ProcessWindowFunciton())
  windowStream.print()

  env.execute()


}

//通过单独定义ProcessWindowFunciton实现ProcessJoinFunction
class ProcessWindowFunciton extends ProcessJoinFunction[(Int, Long), (Int, Long), String] {
  override def processElement(in1: (Int, Long), in2: (Int, Long), context: ProcessJoinFunction[(Int, Long), (Int, Long), String]#Context, collector: Collector[String]): Unit = {
    collector.collect(in1 + ":" + (in1._2 + in2._2))
  }
}