package base

import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.apache.flink.util.Collector
import org.junit.{Assert, Test}

import java.util.Date

// 两个订单流，测试双流Join
case class OrderLogEvent1(orderId: Long, amount: Double, timeStamp: Long)

case class OrderLogEvent2(orderId: Long, itemId: Long, timeStamp: Long)

case class OrderResultEvent(orderId: Long, amount: Double, itemId: Long)

@Test
class JoinTest extends Assert {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val leftOrderStream = env.fromCollection(List(
    OrderLogEvent1(1L, 22.1, 1000L),
    OrderLogEvent1(2L, 22.2, 2000L),
    OrderLogEvent1(4L, 22.3, 4000L),
    OrderLogEvent1(4L, 22.4, 8000L),
    OrderLogEvent1(5L, 22.5, 7000L),
    OrderLogEvent1(6L, 22.6, 6000L)
  ))
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderLogEvent1](Time.seconds(5)) {
      override def extractTimestamp(element: OrderLogEvent1): Long = element.timeStamp
    })
    .keyBy(_.orderId)

  val rightOrderStream = env.fromCollection(List(
    OrderLogEvent2(1L, 121, 1000L),
    OrderLogEvent2(2L, 122, 2000L),
    OrderLogEvent2(3L, 123, 4000L),
    OrderLogEvent2(4L, 124, 8000L),
    OrderLogEvent2(5L, 125, 7000L),
    OrderLogEvent2(7L, 126, 6000L)
  ))
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderLogEvent2](Time.seconds(5)) {
      override def extractTimestamp(element: OrderLogEvent2): Long = element.timeStamp
    })
    .keyBy(_.orderId)


  @Test
  def testInnerJoinByJoin(): Unit = {
    leftOrderStream
      .join(rightOrderStream)
      .where(_.orderId)
      .equalTo(_.orderId)
      .window(TumblingEventTimeWindows.of(Time.minutes(5))) // 5min的时间滚动窗口
      .apply(new InnerJoin)
      .print()

    env.execute()
    assert(true)
  }

  @Test
  def testInnerJoinByCoGroup(): Unit = {
    leftOrderStream
      .coGroup(rightOrderStream)
      .where(_.orderId)
      .equalTo(_.orderId)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .apply(new InnerWindowJoinFunctionC)
    env.execute()
    assert(true)
  }

  @Test
  def testLeftJoinByCoGroup(): Unit = {
    leftOrderStream
      .coGroup(rightOrderStream)
      .where(_.orderId)
      .equalTo(_.orderId)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .apply(new LeftWindowJoinFunction)
    env.execute()
    assert(true)
  }

  @Test
  def testRightJoinByCoGroup(): Unit = {
    leftOrderStream
      .coGroup(rightOrderStream)
      .where(_.orderId)
      .equalTo(_.orderId)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .apply(new RightWindowJoinFunction)
    env.execute()
    assert(true)
  }

  @Test
  def testIntervalJoin(): Unit ={
    leftOrderStream
      .intervalJoin(rightOrderStream)
      .between(Time.minutes(-2), Time.minutes(1))
      .process(new IntervalJoinFunction)
      .print()
    env.execute()
    assert(true)
  }
}

class InnerJoin extends JoinFunction[OrderLogEvent1, OrderLogEvent2, OrderResultEvent] {
  override def join(first: OrderLogEvent1, second: OrderLogEvent2): OrderResultEvent = {
    OrderResultEvent(first.orderId, first.amount, second.itemId)
  }
}

class InnerWindowJoinFunctionC extends CoGroupFunction[OrderLogEvent1, OrderLogEvent2, OrderResultEvent] {
  override def coGroup(first: java.lang.Iterable[OrderLogEvent1],
                       second: java.lang.Iterable[OrderLogEvent2],
                       out: Collector[OrderResultEvent]): Unit = {
    /**
     * 将Java的Iterable对象转化为Scala的Iterable对象
     */
    import scala.collection.JavaConverters._
    val scalaT1 = first.asScala.toList
    val scalaT2 = second.asScala.toList

    // inner join要比较的是同一个key下，同一个时间窗口内的数据
    if (scalaT1.nonEmpty && scalaT1.nonEmpty) {
      for (left <- scalaT1) {
        for (right <- scalaT2) {
          out.collect(OrderResultEvent(left.orderId, left.amount, right.itemId))
        }
      }
    }
  }
}

class LeftWindowJoinFunction extends CoGroupFunction[OrderLogEvent1, OrderLogEvent2, OrderResultEvent] {
  override def coGroup(first: java.lang.Iterable[OrderLogEvent1],
                       second: java.lang.Iterable[OrderLogEvent2],
                       out: Collector[OrderResultEvent]): Unit = {
    /**
     * 将Java的Iterable对象转化为Scala的Iterable对象
     */
    import scala.collection.JavaConverters._
    val scalaT1 = first.asScala.toList
    val scalaT2 = second.asScala.toList

    for (left <- scalaT1) {
      var flag = false // 定义flag，left流中的key在right流中是否匹配
      for (right <- scalaT2) {
        out.collect(OrderResultEvent(left.orderId, left.amount, right.itemId))
        flag = true;
      }
      if (!flag) { // left流中的key在right流中没有匹配到，则给itemId输出默认值0L
        out.collect(OrderResultEvent(left.orderId, left.amount, 0L))
      }
    }
  }
}

class RightWindowJoinFunction extends CoGroupFunction[OrderLogEvent1, OrderLogEvent2, OrderResultEvent] {
  override def coGroup(first: java.lang.Iterable[OrderLogEvent1],
                       second: java.lang.Iterable[OrderLogEvent2],
                       out: Collector[OrderResultEvent]): Unit = {
    /**
     * 将Java的Iterable对象转化为Scala的Iterable对象
     */
    import scala.collection.JavaConverters._
    val scalaT1 = first.asScala.toList
    val scalaT2 = second.asScala.toList
    for (right <- scalaT2) {
      var flag = false // 定义flag，right流中的key在left流中是否匹配

      for (left <- scalaT1) {
        out.collect(OrderResultEvent(left.orderId, left.amount, right.itemId))
        flag = true
      }

      if (!flag) { //没有匹配到的情况
        out.collect(OrderResultEvent(right.orderId, 0.00, right.itemId))
      }

    }
  }
}

class IntervalJoinFunction extends ProcessJoinFunction[OrderLogEvent1,OrderLogEvent2,OrderResultEvent]{
  override def processElement(left: OrderLogEvent1,
                              right: OrderLogEvent2,
                              ctx: ProcessJoinFunction[OrderLogEvent1, OrderLogEvent2, OrderResultEvent]#Context,
                              out: Collector[OrderResultEvent]): Unit = {
    out.collect(OrderResultEvent(left.orderId,left.amount,right.itemId))
  }
}