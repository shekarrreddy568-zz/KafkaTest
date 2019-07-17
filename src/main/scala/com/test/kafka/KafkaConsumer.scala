package com.test.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object KafkaConsumer {

  def main(args: Array[String]): Unit = {

    val TOPIC="WordsWithCountsTopic"

    val  props = new Properties()
    props.put("bootstrap.servers", "hadoop-fra-5.intern.beon.net:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "G1")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Collections.singletonList(TOPIC))

    while(true){
      val records=consumer.poll(100)
      for (record<-records.asScala){
        println(s"key: ${record.key} and value: ${record.value()}")
      }
    }
  }

}
