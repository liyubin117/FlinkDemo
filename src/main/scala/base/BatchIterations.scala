package base


import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.streaming.api.scala._

object BatchIterations extends App{
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val someIntegers: DataStream[Long] = env.generateSequence(0, 20)

  val iteratedStream = someIntegers.iterate(
    iteration => {
      val minusOne = iteration.map( v => v - 1)
      val stillGreaterThanZero = minusOne.filter (_ > 0)
      val lessThanZero = minusOne.filter(_ <= 0)
      (stillGreaterThanZero, lessThanZero)
    }
  )

  iteratedStream.print()
  env.execute()
}
