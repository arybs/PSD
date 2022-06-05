package psd

import org.apache.flink.api.scala._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SuddenTraffic {

}
class MyProcessWindowFunction extends ProcessWindowFunction[SnortReport, String, String, TimeWindow] {

  def process(key: String, context: Context, input: Iterable[SnortReport], out: Collector[String]) = {
    var count = 0L
    println(context, input)
    for (in <- input) {
      count = count + 1
      println(in)
    }
    out.collect(s"Window ${context.window} count: $count")
  }
}

/*
object ProcessFunctionTimers {

  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)

    val warnings = readings
      // key by sensor id
      .keyBy(_.id)
      // apply ProcessFunction to monitor temperatures
      .process(new TempIncreaseAlertFunction)

    warnings.print()

    env.execute("Monitor sensor temperatures.")
  }
}

/** Emits a warning if the temperature of a sensor
 * monotonically increases for 1 second (in processing time).
 */
class TrafficIncreaseAlertFunction
  extends KeyedProcessFunction[String, SnortReport, String] {

  // hold temperature of last sensor reading
  lazy val lastTemp: ValueState[Double] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp", Types.of[Double])
    )

  // hold timestamp of currently active timer
  lazy val currentTimer: ValueState[Long] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

  override def processElement(
                               r: SnortReport,
                               ctx: KeyedProcessFunction[String, SnortReport, String]#Context,
                               out: Collector[String]): Unit = {

    // get previous temperature
    val prevTemp = lastTemp.value()
    // update last temperature
    lastTemp.update(r.temperature)

    val curTimerTimestamp = currentTimer.value()
    if (prevTemp == 0.0) {
      // first sensor reading for this key.
      // we cannot compare it with a previous value.
    }
    else if (r.temperature < prevTemp) {
      // temperature decreased. Delete current timer.
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      currentTimer.clear()
    }
    else if (r.temperature > prevTemp && curTimerTimestamp == 0) {
      // temperature increased and we have not set a timer yet.
      // set timer for now + 1 second
      val timerTs = ctx.timerService().currentProcessingTime() + 1000
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      // remember current timer
      currentTimer.update(timerTs)
    }
  }

  override def onTimer(
                        ts: Long,
                        ctx: KeyedProcessFunction[String, SnortReport, String]#OnTimerContext,
                        out: Collector[String]): Unit = {

    out.collect("Temperature of sensor '" + ctx.getCurrentKey +
      "' monotonically increased for 1 second.")
    // reset current timer
    currentTimer.clear()
  }
}*/
