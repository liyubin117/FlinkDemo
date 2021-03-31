package complex
import java.util.Date

import net.agkn.hll.HLL
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.windowing.time.Time
import sql.EnvDemo.env
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

case class Record(var id:Long, var ts:Long)
object WindowDistinctAggHll {
  def main(args: Array[String]): Unit = {
    println(new Date())
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.socketTextStream("localhost",9888)
      .map(x => {
        val arr = x.split(",")
        Record(arr(0).toLong, arr(1).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Record](Time.seconds(10)) {
        override def extractTimestamp(element: Record): Long = element.ts
      })
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(new HllDistinct())
      .print()

    env.execute()
  }
}

case class Acc(var cnt:Long, var hll:HLL)
class HllDistinct extends AggregateFunction[Record, Acc, (Long, Long)]{
  override def createAccumulator(): Acc = Acc(0, new HLL(14, 6))

  override def add(value: Record, accumulator: Acc): Acc = {
    // todo
    accumulator.cnt = accumulator.cnt + 1
    accumulator.hll.addRaw(value.id)
    accumulator
  }

  override def getResult(accumulator: Acc): (Long, Long) = (accumulator.cnt, accumulator.hll.cardinality())

  override def merge(a: Acc, b: Acc): Acc = {
    a.cnt = a.cnt + b.cnt
    a.hll.union(b.hll)
    a
  }
}
