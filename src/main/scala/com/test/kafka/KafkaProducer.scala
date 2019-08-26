package com.test.kafka

import java.util.{Collections, Properties}

import scala.util.Try
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.clients.producer._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}


object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val mapper = new ObjectMapper
    val topic: String = "test_topic"

    createTopic(topic, 1, 3, getProperties)

    val producer = new KafkaProducer[String, JsonNode](getProperties)
    val key: String = "payment"
    val callback = new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        Option(exception) match {
          case Some(err) => println(s"Failed to produce: $err")
          case None =>  println(s"Topic: ${metadata.topic()}, Partition: ${metadata.partition()}, offset: ${metadata.offset()}")
        }
      }
    }

    (1 to 100).foreach { i =>
      val value =   s"""{"id":$i,"message":Hi$i}"""
      val payload: JsonNode = mapper.valueToTree(value)
     // val payload = JSON.parseFull(value)

      val record = new ProducerRecord[String, JsonNode](topic, key, payload)
      producer.send(record, callback)
      Thread.sleep(1000)
    }

    producer.flush()
    producer.close()

    //  def send(topic: String, payload: String): Unit = {
    //    val record = new ProducerRecord[String, String](topic, key, payload)
    //    producer.send(record)
    //  }
  }

  def getProperties: Properties = {
    val props: Properties = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-fra-5.intern.beon.net:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "test_producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer")
    props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor")
    // props.put("schema.registry.url", "http://schemaregistry:8081")
//    props.put("sasl.mechanism","PLAIN")
//    props.put("security.protocol","SASL_PLAINTEXT")
//    props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafkabroker1\" password=\"kafkabroker1-secret\";")
    props

  }

  def createTopic(topic: String, partitions: Int, replication: Int, Config: Properties): Unit = {
    val newTopic = new NewTopic(topic, partitions, replication.toShort)
    val adminClient = AdminClient.create(Config)
    Try (adminClient.createTopics(Collections.singletonList(newTopic)).all.get).recover {
      case e :Exception =>
        // Ignore if TopicExistsException, which may be valid if topic exists
        if (!e.getCause.isInstanceOf[TopicExistsException]) throw new RuntimeException(e)
    }
    adminClient.close()
  }
}
