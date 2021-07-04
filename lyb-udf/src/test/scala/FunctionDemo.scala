import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{DataTypes, _}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row
import org.junit.{Assert, Before, Test}

@Test
class FunctionDemo extends Assert{
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val bsSettings = EnvironmentSettings.newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()
  val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)

  @Before
  def init()={
    env.setParallelism(1)

    val filePath = "../file/person.csv"
    val schema = new Schema() //定义字段
      .field("id", DataTypes.BIGINT())
      .field("name", DataTypes.STRING())
      .field("age", DataTypes.BIGINT())

    bsTableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv) //定义文件格式
      .withSchema(schema)
      .createTemporaryTable("person")
  }

  @Test
  def table_scalar(): Unit ={
    val hash = new HashCode(2.1)
    bsTableEnv.from("person")
      .select('id, 'name, hash('name) as 'hashcode)
      .toAppendStream[Row]
      .print("scalar function by tableApi")
    env.execute()
    assert(true)
  }

  @Test
  def table_table(): Unit ={
    val split = new Split(":")
    bsTableEnv.from("person")
      .joinLateral(split('name) as ('col1,'col2) )
      .toAppendStream[Row]
      .print("table function by tableApi")
    env.execute()
    assert(true)
  }

  @Test
  def table_aggregate(): Unit ={
    val avg = new Avg
    bsTableEnv.from("person")
      .groupBy('id)
      .aggregate(avg('age) as 'col)
      .select('id, 'col)
      .toRetractStream[Row]
      .print("aggregate function by tableApi")
    env.execute()
    assert(true)
  }

  @Test
  def table_table_aggregate(): Unit ={
    val top2 = new Top2
    bsTableEnv.from("person")
      .groupBy('id)
      .flatAggregate(top2('age) as ('col, 'rank))
      .select('id, 'col, 'rank)
      .toRetractStream[Row]
      .print("table aggregate function by tableApi")
    env.execute()
    assert(true)
  }

  @Test
  def sql_scalar(): Unit ={
    bsTableEnv.registerFunction("hash", new HashCode(2.1))
    bsTableEnv.sqlQuery("select id,name,hash(name) as hashcode from person")
      .toAppendStream[Row]
      .print("scalar function by sqlApi")
    env.execute()
    assert(true)
  }

  @Test
  def sql_table(): Unit ={
    bsTableEnv.registerFunction("split", new Split(":"))
    bsTableEnv.sqlQuery(
      """
        |select id,name,age,word,length
        |from person,
        |lateral table(split(name)) as splitid(word, length)
      """.stripMargin)
      .toAppendStream[Row]
      .print("table function by sqlApi")
    env.execute()
    assert(true)
  }
}

