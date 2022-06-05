package psd

import org.apache.flink.api.common.eventtime.{Watermark, WatermarkGenerator, WatermarkOutput}

import java.lang


/**
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 */
class BoundedOutOfOrdernessGenerator extends WatermarkGenerator[SnortReport] {

  val maxOutOfOrderness = 3500L // 3.5 seconds

  var currentMaxTimestamp: Long = _

  override def onEvent(element: SnortReport, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    currentMaxTimestamp = lang.Long.max(eventTimestamp, currentMaxTimestamp)
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    // emit the watermark as current highest timestamp minus the out-of-orderness bound
    output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
  }

}

/**
 * This generator generates watermarks that are lagging behind processing
 * time by a fixed amount. It assumes that elements arrive in Flink after
 * a bounded delay.
 */
class TimeLagWatermarkGenerator extends WatermarkGenerator[SnortReport] {

  val maxTimeLag = 5000L // 5 seconds

  override def onEvent(element: SnortReport, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    // don't need to do anything because we work on processing time
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag))
  }
}



