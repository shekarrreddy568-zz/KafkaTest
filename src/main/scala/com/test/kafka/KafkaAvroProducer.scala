package com.test.kafka

import java.util.Properties

import com.test.kafka.avro.User
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer._

object KafkaAvroProducer extends LazyLogging {

  def main(args: Array[String]): Unit = {

    logger.info("starting paymentservice...")
    val producer = new KafkaProducer[String, User](getProperties)

    val callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        Option(exception) match {
          case Some(err) => println(s"Failed to produce: $err")
          case None => println(s"Topic: ${metadata.topic()}, Partition: ${metadata.partition()}, offset: ${metadata.offset()}")
        }
      }
    }


    val key: String = "payment"
    while (true) {
      val topic = "payments"
      val r = scala.util.Random
      val id = r.nextInt(10000000)
      val tour_value = r.nextDouble() * 100
      val id_driver = r.nextInt(10)
      val id_passenger = r.nextInt(100)
      val event_date = System.currentTimeMillis

      val payload = User(id, event_date, tour_value, id_driver, id_passenger)
      println(s"payload: ${payload.toString}")

      val record = new ProducerRecord[String, User](topic, key, payload)
      producer.send(record, callback)
      Thread.sleep(1000)
    }
  }

  def getProperties: Properties = {
    val props: Properties = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "confluent-kafka.elisabeth.mytaxi.com:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "test_producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getCanonicalName)
    props.put("schema.registry.url", "https://confluent-schemaregistry.elisabeth.mytaxi.com")
     props.put("sasl.mechanism","PLAIN")
     props.put("security.protocol","SASL_PLAINTEXT")
     props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafkabroker\" password=\"KQ$3J.?3=Kk>:(ZS\";")
    props

  }

}
