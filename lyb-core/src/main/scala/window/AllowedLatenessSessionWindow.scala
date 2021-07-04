package window
import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 对于sessionWindow的情况，当late element在allowedLateness范围之内到达时，可能会引起窗口的merge，这样，之前窗口的数据会在新窗口中累加计算，这就是DataFlow模型中的AccumulatingAndRetracting的情况
  */
object AllowedLatenessSessionWindow extends App{
  val hostName = args(0)
  val port = args(1).toInt


  val env = StreamExecutionEnvironment.getExecutionEnvironment //获取流处理执行环境
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置Event Time作为时间属性
  //env.setBufferTimeout(10)
  //env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)

  val input = env.socketTextStream(hostName,port)

  val inputMap = input.map(f=> {
    val arr = f.split("\\W+")
    val code = arr(0)
    val time = arr(1).toLong
    (code,time)
  })

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

  // allow 5 sec for late element after watermark passed the
  val accumulatorWindow = watermarkDS
    .keyBy(_._1)
    .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
    .allowedLateness(Time.seconds(5))
    .apply(new AccumulatingAndRetractingWindowFunction)
    .name("window accumulate test")
    .setParallelism(1)

  accumulatorWindow.print()
  env.execute()
}

class AccumulatingAndRetractingWindowFunction extends WindowFunction[(String, Long),(String,String,String,Int, String),String,TimeWindow]{

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, String, String, Int, String)]): Unit = {

    // key,window start time, window end time, window size, system time
    out.collect(key,format.format(window.getStart),format.format(window.getEnd),input.size,format.format(System.currentTimeMillis()))

  }
}