package com.test.kafka

import java.util.Properties

import org.apache.kafka.clients.producer._

object KafkaProducer {

  def main(args: Array[String]): Unit = {

    val  props = new Properties()
    props.put("bootstrap.servers", "hadoop-fra-5.intern.beon.net:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC="test000"

    for(i<- 51 to 100){
      val record = new ProducerRecord(TOPIC, "key", s"hello $i")
      producer.send(record)
    }

//    val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
//    producer.send(record)

    producer.close()


  }

}
