package psd

import java.sql.Timestamp
//TODO: Mozna zwiekszysc ilosc analiz, ale porponuje wtedy zmienic case clase (dodac cos w stylu src_port, dst_port itp...
// to się wiąże, z tym, że trzeba dołożyć zmiany w pliku SuddenTraffic. (W out.collect) + match _pattern
// ale imo to sie troche trudniejsze zrobi XD.

case class Report(
  alarmType: String, //Expected: NormalState, AlarmCount, AlarmGrowth
  count: Int, // Number of attempts
  protocol: String,
  ip: String,
  port: String,
  timestamp: Timestamp
)
