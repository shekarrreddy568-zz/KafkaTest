package com.test.kafka

import java.util.{Collections, Properties}

import scala.util.Try
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.clients.producer._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}


object KafkaJsonProducer {

  def main(args: Array[String]): Unit = {
    val mapper = new ObjectMapper
    val topic: String = "raj_test222"

   createTopic(topic, 1, 3, getProperties)

    val producer = new KafkaProducer[String, JsonNode](getProperties)
    val key: String = "payment"
    val callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        Option(exception) match {
          case Some(err) => println(s"Failed to produce: $err")
          case None => println(s"Topic: ${metadata.topic()}, Partition: ${metadata.partition()}, offset: ${metadata.offset()}")
        }
      }
    }

    (1 to 100).foreach { i =>
      val value = s"""{"id":$i,"message":Test Message-ß$i}"""
      val payload: JsonNode = mapper.valueToTree(value)

      val record = new ProducerRecord[String, JsonNode](topic, key, payload)
      producer.send(record, callback)
      println(s"record $payload has been produced successfully")
      Thread.sleep(1000)
    }

    producer.flush()
    producer.close()
  }

  def getProperties: Properties = {
    val props: Properties = new Properties

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.elisabeth.mytaxi.com:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "test_producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer")

//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-cp-kafka-0.staging.otonomousmobility.com:9080")
//    props.put(ProducerConfig.CLIENT_ID_CONFIG, "freenow_test_producer")
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer")
//    props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor")
//    props.put(ProducerConfig.ACKS_CONFIG,"all")
//    props.put("ssl.key.password","OhPiex3Aek5ooXee4Raiquie9ooth2eo")
//    props.put("ssl.keystore.location","/Users/raj.chinthalapellymytaxi.com/Downloads/kafka_mytaxi_staging_certs/kafka.client-mytaxi-staging-shared.keystore.jks")
//    props.put("ssl.keystore.password","OhPiex3Aek5ooXee4Raiquie9ooth2eo")
//    props.put("ssl.truststore.location","/Users/raj.chinthalapellymytaxi.com/Downloads/kafka_mytaxi_staging_certs/kafka.client-mytaxi-staging-shared.truststore.jks")
//    props.put("ssl.truststore.password","OhPiex3Aek5ooXee4Raiquie9ooth2eo")
//    props.put("security.protocol","SSL")
//    props.put("ssl.endpoint.identification.algorithm","")

//    props.put("ssl.key.password","ooMei8oojei2aiY9xua2ooboh8iech2j")
//    props.put("ssl.keystore.location","/Users/raj.chinthalapellymytaxi.com/Downloads/kafka_develop_mytaxi_certs/kafka.client-mytaxi-develop-shared.keystore.jks")
//    props.put("ssl.keystore.password","ooMei8oojei2aiY9xua2ooboh8iech2j")
//    props.put("ssl.truststore.location","/Users/raj.chinthalapellymytaxi.com/Downloads/kafka_develop_mytaxi_certs/kafka.client-mytaxi-develop-shared.truststore.jks")
//    props.put("ssl.truststore.password","ooMei8oojei2aiY9xua2ooboh8iech2j")

    //    props.put("schema.registry.url", "https://confluent-schemaregistry.elisabeth.mytaxi.com")
//    props.put("sasl.mechanism","PLAIN")
//    props.put("security.protocol","SASL_PLAINTEXT")
//    props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafkabroker\" password=\"KQ$3J.?3=Kk>:(ZS\";")
    props

  }

  def createTopic(topic: String, partitions: Int, replication: Int, Config: Properties): Unit = {
    val newTopic = new NewTopic(topic, partitions, replication.toShort)
    val adminClient = AdminClient.create(Config)
    Try(adminClient.createTopics(Collections.singletonList(newTopic)).all.get).recover {
      case e: Exception =>
        // Ignore if TopicExistsException, which may be valid if topic exists
        if (!e.getCause.isInstanceOf[TopicExistsException]) throw new RuntimeException(e)
    }
    adminClient.close()
  }
}
