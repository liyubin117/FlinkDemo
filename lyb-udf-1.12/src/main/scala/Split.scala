import org.apache.flink.table.functions.TableFunction

class Split(separator: String) extends TableFunction[(String, Long)] {
  def eval(str:String) = {
    str.split(separator).foreach(word => collect((word, word.length)))
  }
}
