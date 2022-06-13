import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import psd.{Report, SnortReport, SuddenTraffic}

import java.sql.PreparedStatement
import com.typesafe.config._

object StreamingJob {
  def main(args: Array[String]) {

    //val tmp = ConfigFactory.load("myconfiguration1.conf")
    val conf = ConfigFactory.load("application.conf")
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // logika odczytanie z csv + watermark
    val lineSteam: DataStream[String] = env.readTextFile(conf.getString("app.PathFile"))

    // Mapowanie do Streama z klasą SnortReport
    val SnortLines: DataStream[SnortReport] = lineSteam.map(x => new SnortReport(x))

    // Wykorzystana strategia Watermark (https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/event-time/built_in/#monotonously-increasing-timestamps)
    // Użycie Timestampu z systemu snort. (Uzywany Event time)
    val snortLinesWithTimeStamps = SnortLines.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forMonotonousTimestamps[SnortReport]
        .withTimestampAssigner(new SerializableTimestampAssigner[SnortReport] {
          override def extractTimestamp(element: SnortReport, recordTimestamp: Long): Long = element.timestamp.getTime
        }
    ))

    // Analiza i grupowanie danych

    // Grupowanie po ip

    val ipAnalysis = snortLinesWithTimeStamps.keyBy(_.src)
      .window(TumblingEventTimeWindows.of(Time.seconds(conf.getInt("app.time"))))
      .process(new SuddenTraffic.MyProcessWindowFunction())

    // Grupowanie po protokole

    val protoAnalysis = snortLinesWithTimeStamps.keyBy(_.proto)
      .window(TumblingEventTimeWindows.of(Time.seconds(conf.getInt("app.time"))))
      .process(new SuddenTraffic.MyProcessWindowFunction())


    // Grupowanie po Porcie docelowym
    val portAnalysis = snortLinesWithTimeStamps.filter(_.proto!="ICMP").keyBy(_.dst_port)
      .window(TumblingEventTimeWindows.of(Time.seconds(conf.getInt("app.time"))))
      .process(new SuddenTraffic.MyProcessWindowFunction())


    protoAnalysis.print()
    ipAnalysis.print()
    portAnalysis.print()

    val statementBuilder: JdbcStatementBuilder[Report] = {
    new JdbcStatementBuilder[Report] {
      override def accept(ps: PreparedStatement, t: Report): Unit = {
        ps.setString(1, t.alarmType)
        ps.setInt(2, t.count)
        ps.setString(3, t.protocol)
        ps.setString(4, t.ip)
        ps.setString(5, t.port)
        ps.setTimestamp(6, t.timestamp)
      }
    }
    }
    val connectionOptions: JdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl(conf.getString("app.databaseUrl"))
      .withDriverName("org.postgresql.Driver")
      .withUsername(conf.getString("app.databaseUser"))
      .withPassword(conf.getString("app.databasePassword"))
      .build()

    val executionOptions: JdbcExecutionOptions = JdbcExecutionOptions.builder()
      .withBatchSize(1000)
      .withBatchIntervalMs(200)
      .withMaxRetries(5)
      .build()

    ipAnalysis addSink
      JdbcSink.sink(
        "INSERT INTO protosss (alarmType, count, protocol, ip, port, timestamp) VALUES (?,?,?,?,?,?)",
        statementBuilder,
        executionOptions,
        connectionOptions
      )

    protoAnalysis addSink
      JdbcSink.sink(
        "INSERT INTO protosss (alarmType, count, protocol, ip, port, timestamp) VALUES (?,?,?,?,?,?)",
        statementBuilder,
        executionOptions,
        connectionOptions
      )

    portAnalysis addSink
      JdbcSink.sink(
        "INSERT INTO protosss (alarmType, count, protocol, ip, port, timestamp) VALUES (?,?,?,?,?,?)",
        statementBuilder,
        executionOptions,
        connectionOptions
      )

    env.execute("Network analysis, stream based on Snort logs")
  }
}


