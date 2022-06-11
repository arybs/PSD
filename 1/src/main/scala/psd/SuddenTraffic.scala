package psd

import org.apache.flink.api.scala._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.util.matching.Regex

object SuddenTraffic {
  val TrafficCountLimit: Int = 500 // per 10 s Windows
  val TrafficCountRatio: Int = 5 // 5 times bigger to trigger Alarm
  class MyProcessWindowFunction extends ProcessWindowFunction[SnortReport, Report, String, TimeWindow] {
    /**
     * @param key
     * @param context
     * @param input
     * @param out - zwraca case class Report (zdefiniowana w pliku Report)
     */

    def process(key: String, context: Context, input: Iterable[SnortReport], out: Collector[Report]) = {
      var count = 0L
      for (in <- input) {
        count = count + 1
      }
      val tmp = context.globalState.getState(new ValueStateDescriptor[Int]("lastCount", Types.of[Int]))
      val AlarmName = {
        if (count > TrafficCountLimit) {
          println(s"Number of possible requests exceed limit! Requests: $count")
          "AlarmCount"
        }
        else if (count > TrafficCountRatio * tmp.value()) {
          println(s"Sudden increase of requests! Requests: $count")
          "AlarmGrowth"
        }
        else "NormalState"
      }
      tmp.update(count.toInt)

      val ipPattern: Regex = "(\\d{3}.\\d{3}.\\d{3}.\\d+)".r
      val portPattern: Regex = "(\\d+)".r
      val protocolPattern: Regex = "(\\w+)".r

      val reportTuple = key match {
        case ipPattern(k) => (null, k, null)
        case portPattern(k) => (null, null, k)
        case protocolPattern(k) => (k, null, null)
        case _ => (null, null, null)
        }
      out.collect(Report(AlarmName, count.toInt, reportTuple._1, reportTuple._2, reportTuple._3,
        new Timestamp(context.window.maxTimestamp())))
    }
  }

}

