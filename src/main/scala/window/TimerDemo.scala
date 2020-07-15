package window

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object TimerDemo extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env
    .addSource(new MySourceTuple2)
    .keyBy(_._1)
    .process(new KeyedProcessFunction[String, (String, Long), String] {

      //缓存流数据
      private val cache: mutable.Map[String, ListBuffer[Long]] = mutable.Map[String, ListBuffer[Long]]()
      private var first: Boolean = true

      /**
        * 定时器触发时回调该函数
        *
        * @param timestamp 定时器触发时间
        */
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
        println("定时器触发：" + timestamp)
        //将缓存中的数据组织成需要的格式
        val builder = new StringBuilder()
        for (entry: (String, ListBuffer[Long]) <- cache) {
          builder.append(entry._1).append(":")
          for (ele <- entry._2) {
            builder.append(ele).append(",")
          }
          builder.delete(builder.size - 1, builder.size).append("；")
          cache(entry._1).clear()
        }
        println("定时器注册：" + timestamp)
        //该定时器执行完任务之后，重新注册一个定时器
        ctx.timerService().registerProcessingTimeTimer(timestamp + 5000)
        out.collect(builder.toString())
      }

      /**
        * 处理每一个流数据
        */
      override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
        //仅在该算子接收到第一个数据时，注册一个定时器
        if (first) {
          first = false
          val time: Long = System.currentTimeMillis()
          println("定时器第一次注册：" + time)
          ctx.timerService().registerProcessingTimeTimer(time + 5000)
        }
        //将流数据更新到缓存中
        if (cache.contains(value._1)) {
          cache(value._1).append(value._2)
        } else {
          cache.put(value._1, ListBuffer[Long](value._2))
        }
      }
    }
    )
    .print()
  env.execute()
}



/**
  * 每隔1秒发送一个tuple2类型的数据，第一个字段值为随机的一个姓氏，第二个字段为自增的数字
  **/
class MySourceTuple2 extends SourceFunction[(String, Long)] {

  var isRunning: Boolean = true
  val names: List[String] = List("张", "王", "李", "赵")
  private val random = new Random()
  var number: Long = 1

  override def run(ctx: SourceFunction.SourceContext[(String, Long)]): Unit = {
    while (true) {
      val index: Int = random.nextInt(4)
      ctx.collect((names(index), number))
      number += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}