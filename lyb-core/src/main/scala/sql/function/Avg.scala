package sql.function

import org.apache.flink.table.functions.AggregateFunction

case class AvgAcc(var sum:Long, var num:Int)

class Avg extends AggregateFunction[Long, AvgAcc] {
  override def getValue(accumulator: AvgAcc): Long = {
    if(accumulator.num==0) 0
    else accumulator.sum/accumulator.num
  }

  override def createAccumulator(): AvgAcc = new AvgAcc(0,0)

  def accumulate(acc:AvgAcc, in:Long) = {
    acc.sum += in
    acc.num += 1
  }
}
