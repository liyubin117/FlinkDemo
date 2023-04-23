package org.lyb.sql

import java.util.Collections

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.types.Row
import org.junit.Assert._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object StreamITCase {

  var testResults: mutable.MutableList[String] = mutable.MutableList.empty[String]
  var retractedResults: ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]

  def clear = {
    StreamITCase.testResults.clear()
    StreamITCase.retractedResults.clear()
  }

  def compareWithList(expected: java.util.List[String]): Unit = {
    Collections.sort(expected)
    assertEquals(expected.asScala, StreamITCase.testResults.sorted)
  }

  final class StringSink[T] extends RichSinkFunction[T]() {
    override def invoke(value: T) {
      testResults.synchronized {
        testResults += value.toString
      }
    }
  }

  final class RetractMessagesSink extends RichSinkFunction[(Boolean, Row)]() {
    override def invoke(v: (Boolean, Row)) {
      testResults.synchronized {
        testResults += (if (v._1) "+" else "-") + v._2
      }
    }
  }

  final class RetractingSink() extends RichSinkFunction[(Boolean, Row)] {
    override def invoke(v: (Boolean, Row)) {
      retractedResults.synchronized {
        val value = v._2.toString
        if (v._1) {
          retractedResults += value
        } else {
          val idx = retractedResults.indexOf(value)
          if (idx >= 0) {
            retractedResults.remove(idx)
          } else {
            throw new RuntimeException("Tried to retract a value that wasn't added first. " +
              "This is probably an incorrectly implemented test. " +
              "Try to set the parallelism of the sink to 1.")
          }
        }
      }
    }
  }

}
