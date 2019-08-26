package com.test.kafka

import java.util.{Collections, Properties}

import io.confluent.controlcenter.record.Controlcenter.TopicPartition
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.resource.{Resource, ResourceFilter, ResourceType}

import collection.JavaConverters._
import scala.util.Try

object AdminClientAPI {

  def main(args: Array[String]): Unit = {

    val adminClient = AdminClient.create(getProperties)
    val topic_name = new NewTopic("test666", 1, 1)
    val configs = Map(TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT,
                      TopicConfig.COMPRESSION_TYPE_CONFIG -> "producer",
                      TopicConfig.DELETE_RETENTION_MS_CONFIG -> "100000")
    topic_name.configs(configs.asJava)

    // Create topic
    Try(adminClient.createTopics(List(topic_name).asJavaCollection).all().get()).recover {
      case e: Exception => println(e.getMessage)
      //  if (e.getCause.isInstanceOf[TopicExistsException]) println("topic already exists")
    }

    // Describe Topic
    val topicDescribe = adminClient.describeTopics(Collections.singletonList("test999")).all().get().values().forEach {
      x: TopicDescription => println(s"name: ${x.name()}, partitions: ${x.partitions()}, acl: ${x.authorizedOperations()}, isInternal: ${x.isInternal}")
    }

    // List Topics
    val topics = adminClient.listTopics(new ListTopicsOptions().timeoutMs(500).listInternal(true)).names().get()
    println(s"topics: $topics")

    // Delete Topics
//    Try(adminClient.deleteTopics(Collections.singletonList("test464")).all().get()).recover {
//      case e: Exception => println(e.getMessage)
//    }


    // Alter Topic Config
    // val xxx = Map(new ConfigResource(ConfigResource.Type.TOPIC,"test666"), List(AlterConfigOp(ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG,TopicConfig.CLEANUP_POLICY_COMPACT),AlterConfigOp.OpType.SET)))

    adminClient.alterConfigs(Map(new ConfigResource(ConfigResource.Type.TOPIC,"test666")-> new Config(List(new ConfigEntry("cleanup.policy","compact"),new ConfigEntry("compression.type","gzip")).asJavaCollection)).asJava)

    // Describe topic config
    val des = adminClient.describeConfigs(List(new ConfigResource(ConfigResource.Type.TOPIC,"test666")).asJavaCollection).all().get().values().forEach {
      x: Config => x.entries().forEach(y => println(s"${y.name()}: ${y.value()}"))
    }

    /// List Consumer groups
    val cg = adminClient.listConsumerGroups().all().get()
    cg.forEach {
      x: ConsumerGroupListing => println(x.groupId())
    }

    // Describe consumer groups
    val describeCG = adminClient.describeConsumerGroups(List("my_json_consumer").asJavaCollection).all().get().values()
    describeCG.forEach(x => println(x))

    // New ACL on topics
    val newTopicACL = new AclBinding(new Resource(ResourceType.TOPIC,"test666"), new AccessControlEntry("User:raj","*",AclOperation.ALL,AclPermissionType.ALLOW))
    val aclCreateResult = adminClient.createAcls(List(newTopicACL).asJavaCollection).all().get()
    Try(aclCreateResult).recover {
      case e: Exception => println(e.getMessage)
    }

    // New ACL on cluster level
    val newClusterACL = new AclBinding(new Resource(ResourceType.CLUSTER,"kafka-cluster"), new AccessControlEntry("User:raj","*",AclOperation.ALL,AclPermissionType.ALLOW))
    val aclCreate = adminClient.createAcls(List(newClusterACL).asJavaCollection).all().get()

    // Describe ACL's
    val descACL = new AclBindingFilter(new ResourceFilter(ResourceType.TOPIC,"test666"), AccessControlEntryFilter.ANY)
    val descACLAction = adminClient.describeAcls(descACL).values().get()
    descACLAction.forEach {
      x: AclBinding => println(s"ResourceType: ${x.pattern().resourceType()},name: ${x.pattern().name()}, principal: ${x.entry().principal()}, operation: ${x.entry().operation()}")
    }

    // Delete ACL's
    val delACL = new AclBindingFilter(new ResourceFilter(ResourceType.TOPIC,"test666"), new AccessControlEntryFilter("User:raj","*",AclOperation.ALL,AclPermissionType.ALLOW))
    Try(adminClient.deleteAcls(List(delACL).asJavaCollection).all().get()).recover {
      case e: Exception => println(e.getMessage)
    }

    // Delete Consumer groups
    Try(adminClient.deleteConsumerGroups(List("my_json_consumer").asJavaCollection).all().get()).recover {
      case x: Exception => println(x.getMessage)
    }

    //
//    val par = new TopicPartition("jsontest",0)
//    adminClient.deleteRecords(Map(par,(0,3)), 10)


    // CLUSTER stuff
    val cluster = adminClient.describeCluster()
    val clusterId = cluster.clusterId().get()
    println(s"clusterID: $clusterId")
    val controller = cluster.controller().get()
    println(s"controller: $controller")
    val nodes = cluster.nodes().get()
    println(s"nodes: $nodes")

  }
  def getProperties: Properties = {
    val props: Properties = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-fra-5.intern.beon.net:9092")
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "adminclient")
    props
  }

}
