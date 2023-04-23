package window

import java.text.SimpleDateFormat

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

object SocketWindowWordCount {

  def main(args: Array[String]) : Unit = {

    // the port to connect to
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    // 滑动处理时间窗口
    // get input data by connecting to the socket
//    val text = env.socketTextStream("spark", port, '\n')
//    val windowCounts = text
//      .flatMap { w => w.split("\\s") }
//      .map { w => WordWithCount(w, 1) }
//      .keyBy("word")
//      .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(1))) //滑动处理时间窗口
////      .timeWindow(Time.seconds(5), Time.seconds(1))
//      .sum("count")


    //滑动事件时间窗口
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val text = env.socketTextStream("spark", port, '\n')
    val windowCounts = text
      .map { w => w.split(",") }
      .map { w => WordWithCount(w(0).toLong, w(1), w(2).toLong) }
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[WordWithCount]{
        var currentMaxTimestamp = 0L
        val maxOutOfOrderness = 10000L//最大允许的乱序时间是10s

        var a : Watermark = null

        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        override def getCurrentWatermark: Watermark = {
          new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        }

        def extractTimestamp(t: WordWithCount, l: Long): Long = {
          val timestamp = t.time
          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
          println("timestamp:" + t.time +","+ t.word + "," + t.count +"|" +format.format(t.time) +","+  currentMaxTimestamp + "|"+ format.format(currentMaxTimestamp) + ","+ a.toString)
          timestamp
        }
      })
      .keyBy("word")
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .timeWindow(Time.seconds(5),Time.seconds(1))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  case class WordWithCount(time:Long, word: String, count: Long)
}
