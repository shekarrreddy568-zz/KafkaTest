package com.test.kafka

import java.util
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.StringDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializer

object KafkaAvroConsumer extends LazyLogging {

  def main(args: Array[String]): Unit = {

    logger.info("starting payments consumer application.......")

    val topics = ConfigFactory.load().getString("application.topic.name")

    try {

      val consumer = new KafkaConsumer[String, User](getConsumerProperties) // creating Consumer instance
      consumer.subscribe(util.Collections.singletonList(topics)) // subscribing to the topics

      while (true) {
        val records = consumer.poll(100) // polling for records
        val it = records.iterator()

        while (it.hasNext()) {

          val record = it.next()
          logger.info("key: " + record.key() + " , " + "value: " + record.value())
        }

      }
    }
    catch {
      case e: Exception => logger.info(e.getMessage)
    }
  }

  def getConsumerProperties: Properties = {

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigFactory.load().getString("application.kafka.brokers"))
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, ConfigFactory.load().getString("application.client.id"))
    //  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConfigFactory.load().getString("application.key.deserializer"))
    //  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConfigFactory.load().getString("application.value.deserializer"))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getCanonicalName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, ConfigFactory.load().getString("application.group.id"))
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConfigFactory.load().getString("application.enable.auto.commit"))
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, ConfigFactory.load().getString("application.auto.commit.interval.ms"))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConfigFactory.load().getString("application.auto.offset.reset"))
    props.put("schema.registry.url", ConfigFactory.load().getString("application.schema.registry.url"))
    props.put("specific.avro.reader", ConfigFactory.load().getString("application.specific.avro.reader"))
    props.put("sasl.mechanism","PLAIN")
    props.put("security.protocol","SASL_PLAINTEXT")
    props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafkabroker\" password=\"KQ$3J.?3=Kk>:(ZS\";")

    return props
  }


}
