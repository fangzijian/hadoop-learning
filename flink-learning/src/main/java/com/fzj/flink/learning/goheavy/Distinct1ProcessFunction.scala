package com.fzj.flink.learning.goheavy

import com.fzj.flink.learning.domain.{AdData, AdKey}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * MapState 方式去重, 比较麻烦
 */
class Distinct1ProcessFunction extends KeyedProcessFunction[AdKey, AdData, Void] {
  var devIdState: MapState[String, Int] = _
  var devIdStateDesc: MapStateDescriptor[String, Int] = _
  var countState: ValueState[Long] = _
  var countStateDesc: ValueStateDescriptor[Long] = _

  override def open(parameters: Configuration): Unit = {
    devIdStateDesc = new MapStateDescriptor[String, Int]("devIdState", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[Int]))
    devIdState = getRuntimeContext.getMapState(devIdStateDesc)
    countStateDesc = new ValueStateDescriptor[Long]("countState", TypeInformation.of(classOf[Long]))
    countState = getRuntimeContext.getState(countStateDesc)
  }

  override def processElement(value: AdData, ctx: KeyedProcessFunction[AdKey, AdData, Void]#Context, out: Collector[Void]): Unit = {
    val currW = ctx.timerService().currentWatermark()
    if (ctx.getCurrentKey.time + 1 <= currW) {
      println("late data:" + value)
      return
    }
    val devId = value.devId
    devIdState.get(devId) match {
      case 1 =>
      //表示已经存在
      case _ =>
        devIdState.put(devId, 1)
        val c = countState.value()
        countState.update(c + 1)
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey.time + 1)
    }
    println(countState.value())
  }

  override

  def onTimer(timestamp: Long, ctx: KeyedProcessFunction[AdKey, AdData, Void]#OnTimerContext, out: Collector[Void]): Unit = {
    println(timestamp + " exec clean~~~")
    println(countState.value())
    devIdState.clear()
    countState.clear()
  }
}

object Distinct1ProcessFunction {
  def main(args: Array[String]): Unit = {

  }
}
