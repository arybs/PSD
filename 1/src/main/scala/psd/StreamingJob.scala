
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
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import psd.SnortReport
import psd.BoundedOutOfOrdernessGenerator

import java.time.Duration








object StreamingJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // set up the streaming execution environment
    val linereader = new TextInputFormat(new Path("./"))

    val lineSteam: DataStream[String] = env.readFile[String](linereader, "file:///G:\\Magisterka\\PSD\\Proj2/alert.csv",
      FileProcessingMode.PROCESS_CONTINUOUSLY, 30000L)

    // logika odczytanie z csv + watermarki

    val SnortLines: DataStream[SnortReport] = lineSteam.map(x => new SnortReport(x))

    val snortLinesWithTimeStamps = SnortLines.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[SnortReport](Duration.ofSeconds(30))
        .withTimestampAssigner(new SerializableTimestampAssigner[SnortReport] {
          override def extractTimestamp(element: SnortReport, recordTimestamp: Long): Long =
            element.timestamp.getTime
        }
    ))

    // anliza => wartosci ktore zapisujemy || Logika biznesowa

    // sink - zapis analizy
    SnortLines.print()
    // execute program

    env.execute("Flink Streaming Scala API Skeleton")
  }
}