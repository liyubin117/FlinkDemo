package window

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object WatermarkTest {

  def main(args: Array[String]): Unit = {

    val hostName = "spark"
    val port = 9888

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = env.socketTextStream(hostName,port)

    val inputMap = input
      .filter(!_.isEmpty)
      .map(f=> {
        val arr = f.split("\\W+")
        val code = arr(0)
        val time = arr(1).toLong
        (code,time)
        })

    val watermark = inputMap.assignTimestampsAndWatermarks(new WmAssigner1)

    val window = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunctionTest)

    window.print()

    env.execute()
  }

  class WmAssigner1 extends AssignerWithPeriodicWatermarks[(String,Long)] {

    var currentMaxTimestamp = 0L
    val maxOutOfOrderness = 10000L//最大允许的乱序时间是10s

    var a : Watermark = null

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    override def getCurrentWatermark: Watermark = {
      a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      a
    }

    override def extractTimestamp(t: (String,Long), l: Long): Long = {
      val timestamp = t._2
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
      println("input:" + t._1 +","+ t._2 + " | time:" +format.format(t._2) +" | currentMaxTimestamp:"+ format.format(currentMaxTimestamp) +" | Watermark:"+ format.format(a.getTimestamp))
      timestamp
    }
  }

  class WmAssigner2 extends BoundedOutOfOrdernessTimestampExtractor[(String,Long)](Time.seconds(10)) {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    override def extractTimestamp(t: (String,Long)): Long = {
      val timestamp = t._2
      println("timestamp:" + t._1 +","+ t._2 + "|" +format.format(t._2) +","+ ","+ getCurrentWatermark.toString)
      timestamp
    }
  }

  class WindowFunctionTest extends WindowFunction[(String,Long),(String, Int,String,String,String,String),String,TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int,String,String,String,String)]): Unit = {
      val list = input.toList.sortBy(_._2)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      out.collect(key,input.size,format.format(list.head._2),format.format(list.last._2),format.format(window.getStart),format.format(window.getEnd))
    }
  }


}