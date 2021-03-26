package base

import java.util
import java.util.{Comparator, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

/**
需求：
  按照区域areaId+商品gdsId分组，计算每个分组的累计销售额
  将得到的区域areaId+商品gdsId维度的销售额按照区域areaId分组，然后求得TopN的销售额商品，并且定时更新输出
输入：
orderId01,1573874530000,gdsId03,300,beijing
orderId02,1573874540000,gdsId01,100,beijing
orderId02,1573874540000,gdsId04,200,beijing
orderId02,1573874540000,gdsId02,500,beijing
orderId01,1573874530000,gdsId01,300,beijing

orderId02,1573874540000,gdsId04,500,beijing
  */

case class Order(var orderId: String, var orderTime: Long, var gdsId: String, var amount: Double, var areaId: String)

case class GdsSales(var areaId: String, var gdsId: String, var amount: Double, var orderTime: Long)

object GlobalTopN extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

//  val kafkaConfig = new Properties()
//  kafkaConfig.put("bootstrap.servers", "localhost:9092")
//  kafkaConfig.put("group.id", "test1")
//  val consumer = new FlinkKafkaConsumer[String]("topic1", new SimpleStringSchema(), kafkaConfig)

  val consumer = env.socketTextStream("localhost", 9888)

  val orderStream: DataStream[Order] = consumer
    .map(x => {
      val a = x.split(",")
      Order(a(0), a(1).toLong, a(2), a(3).toDouble, a(4))
    })
  val salesStream: DataStream[GdsSales] = orderStream.keyBy(x => {
    x.areaId + "_" + x.gdsId
  }).process(new KeyedProcessFunction[String, Order, GdsSales]() {
    var orderState: ValueState[Double] = _
    var orderStateDesc: ValueStateDescriptor[Double] = _

    override def open(parameters: Configuration): Unit = {
      orderStateDesc = new ValueStateDescriptor[Double]("order-state", TypeInformation.of(classOf[Double]))
      orderState = getRuntimeContext.getState(orderStateDesc)
    }

    override def processElement(value: Order, ctx: KeyedProcessFunction[String, Order, GdsSales]#Context, out: Collector[GdsSales]): Unit = {
      val currV = orderState.value()
      if (currV == null) {
        orderState.update(value.amount)
      } else {
        val newV = currV + value.amount
        orderState.update(newV)
      }
      out.collect(GdsSales(value.areaId, value.gdsId, orderState.value(), value.orderTime))
    }
  })


  salesStream.keyBy(_.areaId)
    .process(new KeyedProcessFunction[String, GdsSales, Void] {
      var topState: ValueState[java.util.TreeSet[GdsSales]] = _
      var topStateDesc: ValueStateDescriptor[java.util.TreeSet[GdsSales]] = _
      var mappingState: MapState[String, GdsSales] = _
      var mappingStateDesc: MapStateDescriptor[String, GdsSales] = _
      val interval: Long = 60000
      val N: Int = 3
      //定时触发
      var fireState: ValueState[Long] = _
      var fireStateDesc: ValueStateDescriptor[Long] = _

      override def open(parameters: Configuration): Unit = {
        //每个区域的商品销售情况，使用TreeSet去重并从小到大排列
        topStateDesc = new ValueStateDescriptor[java.util.TreeSet[GdsSales]]("top-state", TypeInformation.of(classOf[java.util.TreeSet[GdsSales]]))
        topState = getRuntimeContext.getState(topStateDesc)
        //每个区域每个商品与最新销售情况的对应情况
        mappingStateDesc = new MapStateDescriptor[String, GdsSales]("mapping-state", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[GdsSales]))
        mappingState = getRuntimeContext.getMapState(mappingStateDesc)
        //
        fireStateDesc = new ValueStateDescriptor[Long]("fire-time", TypeInformation.of(classOf[Long]))
        fireState = getRuntimeContext.getState(fireStateDesc)
      }

      override def processElement(value: GdsSales, ctx: KeyedProcessFunction[String, GdsSales, Void]#Context, out: Collector[Void]): Unit = {
        val top = topState.value()
        if (top == null) {
          val topSet: java.util.TreeSet[GdsSales] = new java.util.TreeSet[GdsSales](new Comparator[GdsSales] {
            override def compare(o1: GdsSales, o2: GdsSales): Int = (o1.amount - o2.amount).toInt
          })
          topSet.add(value)
          topState.update(topSet)
          mappingState.put(value.gdsId, value)
        } else {
          mappingState.contains(value.gdsId) match {
            case true => {
              //已经存在该商品的销售数据
              val oldV = mappingState.get(value.gdsId)
              mappingState.put(value.gdsId, value)  //更新商品与最新销售情况的对应
              val values = topState.value()
              values.remove(oldV)
              values.add(value) //更新旧的商品销售数据
              topState.update(values)
            }
            case false => {
              //不存在该商品销售数据
              if (top.size() >= N) {
                //已经达到N 则判断更新
                val min = top.first()
                if (value.amount > min.amount) {
                  top.pollFirst()
                  top.add(value)
                  mappingState.put(value.gdsId, value)
                  topState.update(top)
                }
              } else {
                //还未到达N则直接插入
                top.add(value)
                mappingState.put(value.gdsId, value)
                topState.update(top)
              }
            }
          }
        }
        val currTime = ctx.timerService().currentProcessingTime()
        //1min输出一次
        if (fireState.value() == null) {
          val start = currTime - (currTime % interval)
          val nextFireTimestamp = start + interval
          ctx.timerService().registerProcessingTimeTimer(nextFireTimestamp)
          fireState.update(nextFireTimestamp)
        }

      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, GdsSales, Void]#OnTimerContext, out: Collector[Void]): Unit = {
        println(timestamp + "===")
        val c: util.TreeSet[GdsSales] = topState.value()
        c.foreach(x => println(x))
        val fireTimestamp = fireState.value()
        if (fireTimestamp != null && (fireTimestamp == timestamp)) {
          fireState.clear()
          fireState.update(timestamp + interval)
          ctx.timerService().registerProcessingTimeTimer(timestamp + interval)
        }
      }

    })


  env.execute()
}
