
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.eventtime.{BoundedOutOfOrdernessWatermarks, SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineFormat
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import psd.{SuddenTraffic, SnortReport}

import java.time.Duration








object StreamingJob {
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // logika odczytanie z csv + watermark
    val linereader = new TextInputFormat(new Path("./"))

    //val lineSteam: DataStream[String] = env.readFile[String](linereader, "file:///G:\\Magisterka\\PSD\\Proj2/alert.csv",
      //FileProcessingMode.PROCESS_CONTINUOUSLY, 30000L)
    val lineSteam: DataStream[String] = env.readTextFile("./alert.csv") //TODO: Zmienic sciezke na parametr

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
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new SuddenTraffic.MyProcessWindowFunction())

    // Grupowanie po protokole

    val protoAnalysis = snortLinesWithTimeStamps.keyBy(_.proto)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new SuddenTraffic.MyProcessWindowFunction())


    // Grupowanie po Porcie docelowym
    val portAnalysis = snortLinesWithTimeStamps.keyBy(_.dst_port.toString)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new SuddenTraffic.MyProcessWindowFunction())

    protoAnalysis.print()
    ipAnalysis.print()
    portAnalysis.print()


    // sink - zapis analizy
    //SnortLines.print()
    // execute program

    env.execute("Network analysis, stream based on Snort logs")
  }
}


