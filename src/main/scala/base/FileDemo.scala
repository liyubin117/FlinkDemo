//package base
//
//import Table.Person
//import org.apache.flink.api.scala._
//
//object FileDemo extends App{
//  val env  = ExecutionEnvironment.getExecutionEnvironment
//
//  /**
//    * 读取文件
//    */
//  val textFile = "file/text.txt"
//  val csvFile = "file/person.csv"
//
//  // read text file from local files system
//  val localLines = env.readTextFile(textFile)
//  localLines.print()
//
//  // read text file from a HDFS running at nnHost:nnPort
//  val hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile")
//
//  // read a CSV file with three fields
//  val csvInput1 = env.readCsvFile[(Int, String, Int)](csvFile)
//  csvInput1.print()
//
//  // read a CSV file with five fields, taking only two of them
//  val csvInput2 = env.readCsvFile[(String, Double)](
//    csvFile,
//    includedFields = Array(0, 2)) // take the first and the third field
//  csvInput2.print()
//
//  // CSV input can also be used with Case Classes
//  case class MyCaseClass(id: String, age: Int)
//  val csvInput3 = env.readCsvFile[MyCaseClass](
//    csvFile,
//    includedFields = Array(0, 2)) // take the first and the fourth field
//  csvInput3.print()
//
//  // read a CSV file with three fields into a POJO (Person) with corresponding fields
//  val csvInput4 = env.readCsvFile[Person](
//    csvFile,
//    pojoFields = Array("id", "name"))
//  csvInput4.print()
//  println(csvInput4.collect()(0).age)
//
//  // create a set from some given elements
//  val values = env.fromElements("Foo", "bar", "foobar", "fubar")
//  values.print()
//
//  // generate a number sequence
//  val numbers = env.generateSequence(1, 3)
//  numbers.print()
//
////  // read a file from the specified path of type TextInputFormat
////  val tuples = env.readHadoopFile(new TextInputFormat, classOf[LongWritable],
////    classOf[Text], "hdfs://nnHost:nnPort/path/to/file")
////
////  // read a file from the specified path of type SequenceFileInputFormat
////  val tuples = env.readSequenceFile(classOf[IntWritable], classOf[Text],
////    "hdfs://nnHost:nnPort/path/to/file")
//}
