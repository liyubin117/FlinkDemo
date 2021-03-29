package complex

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
  * 需求：
  * 按照区域areaId+商品gdsId分组，计算每个分组的累计销售额
  * 将得到的区域areaId+商品gdsId维度的销售额按照区域areaId分组，然后求得TopN的销售额商品，并且定时更新输出
  * 输入：
  * orderId01,1573874530000,gdsId03,300,beijing
  * orderId02,1573874540000,gdsId01,100,beijing
  * orderId02,1573874540000,gdsId04,200,beijing
  * orderId02,1573874540000,gdsId02,500,beijing
  * orderId01,1573874530000,gdsId01,300,beijing
  * *
  * orderId02,1573874540000,gdsId04,500,beijing
  * *
  * orderId02,1573874540000,gdsId03,10000,beijing
  * *
  * orderId02,1573874540000,gdsId05,20000,beijing
  * *
  * orderId02,1573874540000,gdsId01,20000,beijing
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
      var topState: ValueState[java.util.TreeSet[GdsSales]] = _ //top n
      var topStateDesc: ValueStateDescriptor[java.util.TreeSet[GdsSales]] = _
      var mappingState: MapState[String, GdsSales] = _ // 商品名与对应情况的映射
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
        //一开始流入元素时，top状态是空的，要初始化并把第一个元素写进状态
        // 后面流入元素时，首先把top状态里的同商品元素旧状态删掉，然后更新商品名与元素的映射状态，若top未达到N则直接写入，若达到N则与第一个元素进行比较若大于则覆盖
        val top = topState.value()
        if (top == null) {
          val topSet: java.util.TreeSet[GdsSales] = new java.util.TreeSet[GdsSales](new Comparator[GdsSales] {
            override def compare(o1: GdsSales, o2: GdsSales): Int = (o1.amount - o2.amount).toInt
          })
          topSet.add(value)
          topState.update(topSet)
        } else {
          if(mappingState.contains(value.gdsId)) top.remove(mappingState.get(value.gdsId))
          mappingState.put(value.gdsId, value)
          if (top.size() < N) {
            //还未到达N则直接插入
            top.add(value)
            topState.update(top)
          } else {
            //已经达到N 则判断更新
            val min = top.first()
            if (value.amount > min.amount) {
              top.pollFirst()
              top.add(value)
              topState.update(top)
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

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, GdsSales, Void]#OnTimerContext, out: Collector[Void]): Unit

      = {
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

    }

    )


  env.execute()
}
