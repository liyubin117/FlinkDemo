package sql.function

import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

case class Top2Acc(var first:Long = Long.MinValue, var second:Long = Long.MinValue)
class Top2 extends TableAggregateFunction[(Long,Int), Top2Acc]{
  override def createAccumulator(): Top2Acc = new Top2Acc()
  def accumulate(acc:Top2Acc, in:Long):Unit = {
    if(in>acc.first){
      acc.second = acc.first
      acc.first = in
    }else if(in>acc.second){
      acc.second = in
    }
  }
  def emitValue(acc:Top2Acc, out:Collector[(Long,Int)]):Unit = {
    out.collect((acc.first, 1))
    out.collect((acc.second, 2))
  }
}
