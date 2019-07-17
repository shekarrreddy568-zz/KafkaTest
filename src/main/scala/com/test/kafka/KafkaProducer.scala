package com.test.kafka

import java.util.Properties
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object KafkaProducer {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-fra-5.intern.beon.net:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "payment_producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
   // props.put("schema.registry.url", "http://schemaregistry:8081")

    val producer = new KafkaProducer[String, String](props)

    val key: String = "payment"
    val topic: String = "payments_topic456"

    //  def send(topic: String, payload: String): Unit = {
    //    val record = new ProducerRecord[String, String](topic, key, payload)
    //    producer.send(record)
    //  }

    (1 to 100).foreach { i =>
      val payload = s"message $i"
      val record = new ProducerRecord[String, String](topic, key, payload)
      producer.send(record)
      println(s"message has been producer to topic: $payload")
      Thread.sleep(1000)
    }

  }
}
