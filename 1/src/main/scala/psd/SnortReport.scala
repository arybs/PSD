package psd

import java.sql.Timestamp


class SnortReport (var timestamp: Timestamp, var msg: String, var proto: String, var src: String, var src_port: String,
                   var dst: String, var dst_port: String)
{
  // one-arg auxiliary constructor
  def this(SnortReportLine: String) = {
    this(utils.toTimeStamp(SnortReportLine.split(",")(0)), msg = SnortReportLine.split(",")(1),
      proto = SnortReportLine.split(",")(2), src = SnortReportLine.split(",")(3),
      src_port= SnortReportLine.split(",")(4), dst = SnortReportLine.split(",")(5),
      dst_port = SnortReportLine.split(",")(6))
  }

  override def toString = s"$msg source: $src timestamp: $timestamp"

}

