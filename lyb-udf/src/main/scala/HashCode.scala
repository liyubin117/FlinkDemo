import org.apache.flink.table.functions.ScalarFunction

class HashCode(factor:Double) extends ScalarFunction{
  def eval(in:String): Int = (in.hashCode * factor).toInt
}