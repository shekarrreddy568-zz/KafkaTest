package com.test.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object KafkaStreamsApp {

  def main(args: Array[String]): Unit = {
    import Serdes._

    val props = new Properties()
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-fra-5.intern.beon.net:9092")
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Streams_Test")
    //props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String.getClass.getName)
   // props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String.getClass.getName)


    val builder: StreamsBuilder = new StreamsBuilder
    val textLines: KStream[String, String] = builder.stream[String, String]("payments_topic456")
//    val xxx: KStream[String, String] = textLines.mapValues(value => value.contains("1").toString)
//    xxx.to("kst123")

    textLines.foreach{(key, value) =>
      println(key.toString, value.toString)
      }

    val wordCounts: KTable[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
      .groupBy((_, word) => word)
      .count()(Materialized.as("counts-store"))
    wordCounts.toStream.to("WordsWithCountsTopic")

    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(1000000000, TimeUnit.SECONDS)
    }

  }

}
