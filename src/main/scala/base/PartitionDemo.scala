package base
import org.apache.flink.api.common.functions.Partitioner
import sql.EnvDemo.env

object PartitionDemo extends App{
  env.setParallelism(2)
  import org.apache.flink.api.scala._
  val text = env.socketTextStream("localhost", 9888)
  val tupdata = text.map(line=>{Tuple1(line)})
  val partdata = tupdata.partitionCustom(new MyPartition,0)
  val result = partdata.map(line => {
    println("当前线程id" + Thread.currentThread().getId + ",value" + line)
    line._1
  })
  result.print().setParallelism(1)

  env.execute("StreamingDemoWithMyPartition")
}

class MyPartition extends Partitioner[Long]{
  override def partition(key: Long, numpartition: Int): Int = {
    System.out.println("总的分区数"+numpartition)
    if (key%2==0){
      0
    }else{
      1
    }
  }
}
