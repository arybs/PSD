package psd

import java.sql.Timestamp


case class Report(
  alarmType: String, //Expected: NormalState, AlarmCount, AlarmGrowth
  count: Int, // Number of attempts
  protocol: String,
  ip: String,
  port: String,
  timestamp: Timestamp
)
