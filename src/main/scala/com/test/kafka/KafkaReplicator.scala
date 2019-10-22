package com.test.kafka

import java.util
import java.util.Properties

import com.test.kafka.avroClients.User
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._


object KafkaReplicator extends Thread with LazyLogging {

  def main(args: Array[String]): Unit = {

    logger.info("Kafka replicator app is starting......")
    val th1 = new MyThread("th1","th1.copy")
    val th2 = new MyThread("th2", "th2.copy")
    th1.start()
    th2.start()

  }

  class MyThread (srcTopic:String,dstTopic:String) extends Thread {
    override def run() {
      println(s"repl starting in thread: ${Thread.currentThread().getName()}")

      val topicToConsume = srcTopic
      val topicToProduce = dstTopic


      val consumer = new KafkaConsumer[String, User](getConsumerProperties) // creating Consumer instance
      consumer.subscribe(util.Collections.singletonList(topicToConsume)) // subscribing to the topics

      val producer = new KafkaProducer[String, User](getProducerProperties)
      val callback = new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          Option(exception) match {
            case Some(err) => println(s"Failed to produce: $err")
            case None => println(s"Topic: ${metadata.topic()}, Partition: ${metadata.partition()}, offset: ${metadata.offset()}")
          }
        }
      }

      try {
        while (true) {

          val records = consumer.poll(100) // polling for records
          val it = records.iterator()

          while (it.hasNext) {

            val consumer_record = it.next()
            println("key: " + consumer_record.key() + " , " + "value: " + consumer_record.value())
            val producer_record = new ProducerRecord[String, User](topicToProduce, consumer_record.value())

            producer.send(producer_record, callback)
            consumer.commitSync()
          }
        }
      }
      catch {
        case e: Exception => println(e.getMessage)
      }
    }
  }

  def getConsumerProperties: Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigFactory.load().getString("replicator.consumerBootStrapServers"))
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, ConfigFactory.load().getString("replicator.consumerClientId"))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConfigFactory.load().getString("replicator.ConsumerKeyDeserializer"))
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConfigFactory.load().getString("replicator.ConsumerValueDeserializer"))
    props.put(ConsumerConfig.GROUP_ID_CONFIG, ConfigFactory.load().getString("replicator.ConsumerGroupId"))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,ConfigFactory.load().getString("replicator.ConsumerAutoOffsetReset"))
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConfigFactory.load().getBoolean("replicator.ConsumerEnableAutoCommit"))
    props.put("schema.registry.url", ConfigFactory.load().getString("replicator.ConsumerSchemaRegistryUrl"))
    props
  }

  def getProducerProperties: Properties = {
    val props: Properties = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigFactory.load().getString("replicator.ProducerBootStrapServers"))
    props.put(ProducerConfig.CLIENT_ID_CONFIG, ConfigFactory.load().getString("replicator.ProducerClientId"))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ConfigFactory.load().getString("replicator.ProducerKeySerializer"))
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigFactory.load().getString("replicator.ProducerValueSerializer"))
    props.put("schema.registry.url", ConfigFactory.load().getString("replicator.ProducerSchemaRegistryUrl"))
    props.put("sasl.mechanism", ConfigFactory.load().getString("replicator.ProducerSASLMechanism"))
    props.put("security.protocol", ConfigFactory.load().getString("replicator.ProducerSecurityProtocol"))
    props.put("sasl.jaas.config", ConfigFactory.load().getString("replicator.ProducerSASLJaasConf"))
    props
  }
}
