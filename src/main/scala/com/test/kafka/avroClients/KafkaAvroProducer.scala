package com.test.kafka.avroClients

import java.util.Properties

import com.test.kafka.avro.User
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

object KafkaAvroProducer extends LazyLogging {

  def main(args: Array[String]): Unit = {

    logger.info("starting paymentservice...")
    val producer = new KafkaProducer[String, User](getProperties)

    val callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        Option(exception) match {
          case Some(err) => logger.info(s"Failed to produce: $err")
          case None => logger.info(s"Topic: ${metadata.topic()}, Partition: ${metadata.partition()}, offset: ${metadata.offset()}")
        }
      }
    }


    val key: String = "payment"
    while (true) {
      val topic = "freenow.compensationservice.test"
      val r = scala.util.Random
      val id = r.nextInt(10000000)
      val tour_value = r.nextDouble() * 100
      val id_driver = r.nextInt(10)
      val id_passenger = r.nextInt(100)
      val event_date = System.currentTimeMillis

      val payload = User(id, event_date, tour_value, id_driver, id_passenger)
      logger.info(s"payload: ${payload.toString}")

      val record = new ProducerRecord[String, User](topic, key, payload)
      producer.send(record)
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
       props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor")
    props.put("confluent.monitoring.interceptor.sasl.mechanism","PLAIN")
    props.put("confluent.monitoring.interceptor.security.protocol", "SASL_PLAINTEXT")
    props.put("confluent.monitoring.interceptor.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"compensationservice\" password=\"JuqjIUSHybpqZai\";")
    props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"compensationservice\" password=\"JuqjIUSHybpqZai\";")

//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.elisabeth.mytaxi.com:9092")
//    props.put(ProducerConfig.CLIENT_ID_CONFIG, "test_producer")
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getCanonicalName)
//    props.put("schema.registry.url", "https://schemaregistry.elisabeth.mytaxi.com")
//    props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor")

    props

  }
}
