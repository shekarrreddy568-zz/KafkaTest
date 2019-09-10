package com.test.kafka

import java.util
import java.util.{Collections, Properties}
import java.time.Duration
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

object KafkaJsonConsumer {

  def main(args: Array[String]): Unit = {

    val topics = "test_topic"
    val consumer = new KafkaConsumer[String, String](getProperties) // creating Consumer instance
    consumer.subscribe(Collections.singletonList(topics)) // subscribing to the topics

    println("polling")
    while (true) {
      val records = consumer.poll(Duration.ofSeconds(1)) // polling for records
      val it = records.iterator()

      while (it.hasNext()) {

        val record = it.next()
        println("key: " + record.key() + " , " + "value: " + record.value())
      }

    }
    consumer.close()
}
  def getProperties: Properties = {
    val props: Properties = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-fra-5.intern.beon.net:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer")
    props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor")
    // props.put("schema.registry.url", "http://schemaregistry:8081")
    props

  }

}
