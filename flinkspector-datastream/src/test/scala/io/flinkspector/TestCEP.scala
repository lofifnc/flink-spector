package io.flinkspector.test_cep

import io.flinkspector.core.collection.ExpectedRecords
import io.flinkspector.datastream.{DataStreamExtendedBase, DataStreamTestBase}
import org.junit.Test
import org.apache.flink.cep.{CEP, PatternSelectFunction}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.junit.runner.RunWith
import org.junit.runners.{BlockJUnit4ClassRunner, JUnit4}
import org.scalatest.junit.AssertionsForJUnit



class TestCEP extends DataStreamTestBase with AssertionsForJUnit {

  @Test
  def testWithFlinkspectorAndJavaStreams(): Unit = {
    val event1 = Event("xxx", "", "message")
    val event2 = Event("xxx", "", "web portal")
    val event3 = Event("xxx", "", "message")
    val event4 = Event("xxx", "", "message")
    val event5 = Event("xxx", "", "call")


    val input = createTimedTestStreamWith(event1).emit(event2).emit(event3).emit(event4).emit(event5).close()

    val pattern = Pattern.begin("event0").subtype(classOf[Event]).where(new EventActionCondition("message")).next("event1").where(new EventActionCondition("call"))

    val patternStream = CEP.pattern(input, pattern)
    val alertStream: DataStream[String] = patternStream.select(new AlertPatternSelectFunction())

    val output: ExpectedRecords[String] = ExpectedRecords.create[String]("Got one !")
    output.refine().only()
    assertStream(alertStream, output)
  }
}

case class Event(one: String, two: String, action: String)

class EventActionCondition(action: String) extends IterativeCondition[Event] {
  override def filter(value: Event, ctx: IterativeCondition.Context[Event]): Boolean = {
    println("called")
    value.action == action
  }
}

class AlertPatternSelectFunction extends PatternSelectFunction[Event, String] {
  override def select(pattern: java.util.Map[String, java.util.List[Event]]): String = {
    "got it !"
  }
}