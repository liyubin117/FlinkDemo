package window

import java.text.SimpleDateFormat
import java.util.Date

object TimestampTest extends App{
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  println(sdf.format(System.currentTimeMillis()))
  println(System.currentTimeMillis())

  val sformat = new SimpleDateFormat("ss")
  println(sformat.format(System.currentTimeMillis()).toLong)

  println((23/3f).floor)
  println((23/3f).ceil)

  val begin = System.currentTimeMillis()
  println("1,"+(begin+1000))
  println("1,"+(begin+5000))
  println("1,"+(begin+11000))
  println("1,"+(begin+12000))
  println("1,"+(begin+13000))
  println("1,"+(begin+14000))
  println("1,"+(begin+15000))
}
