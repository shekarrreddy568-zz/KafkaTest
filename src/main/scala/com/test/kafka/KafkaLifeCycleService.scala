package com.test.kafka

import java.util.{Dictionary, Properties}

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, Config, ConfigEntry, NewTopic}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.TopicExistsException

import scala.util.Try
import collection.JavaConverters._
import org.yaml.snakeyaml.Yaml
import java.io.{File, FileInputStream}
import java.util
import play.libs
import org.yaml.snakeyaml.constructor.Constructor

object KafkaLifeCycleService {

  def main(args: Array[String]): Unit = {

    val adminClient = AdminClient.create(getProperties)
    val topic_name = "test001"
    createTopic(adminClient, topic_name, 1, 1)
    alterTopicConfig(adminClient, topic_name)
    describeTopicConfig(adminClient, topic_name)
  }

  def getProperties: Properties = {
    val props: Properties = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-fra-5.intern.beon.net:9092")
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "adminclient")
    props
  }

  def createTopic(a: AdminClient, topic: String, partitions: Int, replicationFactor: Int): Try[Any] = {
    val topicObject = new NewTopic(topic, partitions, replicationFactor.toShort)

    Try(a.createTopics(List(topicObject).asJavaCollection).all().get()).recover {
      case e: Exception =>
      if (e.getCause.isInstanceOf[TopicExistsException]) println("topic already exists")
        else println(e.getMessage)
    }
  }

  def alterTopicConfig(a: AdminClient, topic: String) = {
    val topic_name = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    val topicProps = new Config(
      List(
        new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "compact"),
        new ConfigEntry(TopicConfig.DELETE_RETENTION_MS_CONFIG, "1000"),
        new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
      ).asJavaCollection)

    try {
      a.alterConfigs(Map(topic_name -> topicProps).asJava).all().get()
    } catch {
      case e: Exception => println(e.getMessage)
      }
  }

  def describeTopicConfig(a: AdminClient, topic: String): Unit = {
    lazy val topicConfig = a.describeConfigs(List(new ConfigResource(ConfigResource.Type.TOPIC,topic)).asJavaCollection).all().get().values()
      topicConfig.forEach {
      x: Config => x.entries().forEach(y => println(s"${y.name()}: ${y.value()}"))
    }
  }

}
