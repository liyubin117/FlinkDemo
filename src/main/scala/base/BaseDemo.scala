//package base
//
//import org.apache.flink.streaming.api.scala._
//
//object BaseDemo extends App{
//  val env = StreamExecutionEnvironment.getExecutionEnvironment
//  env.setParallelism(1)
//
//  //split分割流 select选择流
//  val input: DataStream[Int] = env.fromElements(
//    1,2,3,4,5
//  )
//  val split = input.split(x=>{
//    x%2 match {
//      case 0 => List("even")
//      case 1 => List("odd")
//    }
//  })
//
////  split.select("even").print()
////  split.select("odd").print()
//  split.select("even","odd").print()
//
//  env.execute()
//}
