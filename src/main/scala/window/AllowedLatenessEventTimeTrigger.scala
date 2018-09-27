package window
import java.text.SimpleDateFormat

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 对于trigger是默认的EventTimeTrigger的情况下，allowedLateness会再次触发窗口的计算，而之前触发的数据，会buffer起来，直到watermark超过end-of-window + allowedLateness（）的时间，窗口的数据及元数据信息才会被删除。再次计算就是DataFlow模型中的Accumulating的情况
  */
object AllowedLatenessEventTimeTrigger extends App{
  val hostName = "spark"
  val port = 9888


  val env = StreamExecutionEnvironment.getExecutionEnvironment //获取流处理执行环境
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置Event Time作为时间属性
  //env.setBufferTimeout(10)
  //env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)

  val input = env.socketTextStream(hostName,port) //socket接收数据

  val inputMap = input.map(f=> {
    val arr = f.split("\\W+")
    val code = arr(0)
    val time = arr(1).toLong
    (code,time)
  })

  /**
    * 允许3秒的乱序
    */
  val watermarkDS = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)] {

    var currentMaxTimestamp = 0L
    val maxOutOfOrderness = 3000L

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    override def getCurrentWatermark: Watermark = {
      new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    }

    override def extractTimestamp(t: (String,Long), l: Long): Long = {
      val timestamp = t._2
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
      timestamp
    }
  })

  /**
    * 对于此窗口而言，允许5秒的迟到数据，即第一次触发是在watermark > end-of-window时
    * 第二次（或多次）触发的条件是watermark < end-of-window + allowedLateness时间内，这个窗口有late数据到达
    */
  val accumulatorWindow = watermarkDS
    .keyBy(_._1)
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .allowedLateness(Time.seconds(5))
    .apply(new AccumulatingWindowFunction)
    .name("window accumulate test")
    .setParallelism(1)

  accumulatorWindow.print()
  env.execute()
}

class AccumulatingWindowFunction extends RichWindowFunction[(String, Long),(String,String,String,Int, String, Int),String,TimeWindow]{

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  var state: ValueState[Int] = _

  var count = 0

  override def open(config: Configuration): Unit = {
    state = getRuntimeContext.getState(new ValueStateDescriptor[Int]("AccumulatingWindow Test", classOf[Int], 0))
  }

  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, String, String, Int, String, Int)]): Unit = {
    count = state.value() + input.size

    state.update(count)

    // key,window start time, window end time, window size, system time, total size
    out.collect(key, format.format(window.getStart),format.format(window.getEnd),input.size, format.format(System.currentTimeMillis()),count)
  }
}