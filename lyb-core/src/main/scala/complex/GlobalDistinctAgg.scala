package complex

import java.nio.charset.Charset

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import sql.EnvDemo.env
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

case class ResultElement (in:String, num:Long)
object GlobalDistinctAgg {
  def main(args: Array[String]): Unit = {
    //使用布隆过滤器非精准去重聚合
    env.socketTextStream("localhost", 9888)
      .keyBy(x => x.split(",")(0))
      .process(new KeyedProcessFunction[String, String, ResultElement] {
        var bfState:ValueState[BloomFilter[String]] = _
        var bfDescriptor:ValueStateDescriptor[BloomFilter[String]] = _
        var countState:ValueState[Long] = _
        var countDescriptor:ValueStateDescriptor[Long] = _

        override def open(parameters: Configuration): Unit = {
          bfDescriptor = new ValueStateDescriptor[BloomFilter[String]]("bf", TypeInformation.of(classOf[BloomFilter[String]]))
          bfState = getRuntimeContext.getState(bfDescriptor)
          countDescriptor = new ValueStateDescriptor[Long]("count", TypeInformation.of(classOf[Long]))
          countState = getRuntimeContext.getState(countDescriptor)
        }

        override def processElement(value: String, ctx: KeyedProcessFunction[String, String, ResultElement]#Context, out: Collector[ResultElement]): Unit = {
          var count = countState.value()
          var bf = bfState.value()
          val in = value.split(",")(1)
          if(count == null){
            count = 0
          }
          if(bf == null){
            bf = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1000000)
          }
          if(!bf.mightContain(in)){
            count = count + 1
            bf.put(in)
          }
          countState.update(count)
          bfState.update(bf)
          out.collect(ResultElement(value, count))
        }
      })
      .print()
    env.execute()
  }

}
