package org.lyb.sql

import java.io.{File, FileOutputStream, OutputStreamWriter}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sinks.{CsvTableSink, RetractStreamTableSink}
import org.apache.flink.table.sources.CsvTableSource
import org.junit.Test

class TemporalTableJoin {

  @Test
  def test(): Unit = {
    // Streaming 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    //方便我们查出输出数据
    env.setParallelism(1)

    val sourceTableName = "RatesHistory"
    // 创建CSV source数据结构
    val tableSource = genRatesHistorySource
    // 注册source
    tEnv.registerTableSource(sourceTableName, tableSource)

    // 注册retract sink
    val sinkTableName = "retractSink"
    val fieldNames = Array("rowtime", "currency", "rate")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING, Types.STRING)

    tEnv.registerTableSink(
      sinkTableName,
      fieldNames,
      fieldTypes,
      new PaulRetractStreamTableSink)

    val SQL =
      """
        |SELECT *
        |FROM RatesHistory AS r
        |WHERE r.rowtime = (
        |  SELECT MAX(rowtime)
        |  FROM RatesHistory AS r2
        |  WHERE r2.currency = r.currency
        |  AND r2.rowtime <= '10:58:00'  )
      """.stripMargin

    // 执行查询
    val result = tEnv.sqlQuery(SQL)

    // 将结果插入sink
    result.insertInto(sinkTableName)
    env.execute()
  }


  def genRatesHistorySource: CsvTableSource = {

    val csvRecords = Seq(
      "rowtime ,currency   ,rate",
      "09:00:00   ,US Dollar  , 102",
      "09:00:00   ,Euro       , 114",
      "09:00:00  ,Yen        ,   1",
      "10:45:00   ,Euro       , 116",
      "11:15:00   ,Euro       , 119",
      "11:49:00   ,Pounds     , 108"
    )
    // 测试数据写入临时文件
    val tempFilePath =
      writeToTempFile(csvRecords.mkString("$"), "csv_source_", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("rowtime","currency","rate"),
      Array(
        Types.STRING,Types.STRING,Types.STRING
      ),
      fieldDelim = ",",
      rowDelim = "$",
      ignoreFirstLine = true,
      ignoreComments = "%"
    )
  }

  def writeToTempFile(
                       contents: String,
                       filePrefix: String,
                       fileSuffix: String,
                       charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tmpWriter.write(contents)
    tmpWriter.close()
    println(tempFile.getAbsolutePath)
    tempFile.getAbsolutePath
  }

}
