package complex
import net.agkn.hll.HLL
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.windowing.time.Time
import sql.EnvDemo.env
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

case class Record(var id:Long, var ts:Long)
class WindowDistinctAggHll {
  def main(args: Array[String]): Unit = {
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
  }
}

class HllDistinct extends AggregateFunction[Record, Tuple2[Long, HLL], Tuple2[Long, Long]]{
  override def createAccumulator(): (Long, HLL) = Tuple2(0, new HLL(14, 6))

  override def add(value: Record, accumulator: (Long, HLL)): (Long, HLL) = {
    // todo
    accumulator._1 = accumulator._1 + 1

  }

  override def getResult(accumulator: (Long, HLL)): (Long, Long) = ???

  override def merge(a: (Long, HLL), b: (Long, HLL)): (Long, HLL) = ???
}
