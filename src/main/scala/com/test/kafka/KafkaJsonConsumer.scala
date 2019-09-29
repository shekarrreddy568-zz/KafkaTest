package com.test.kafka

import java.util
import java.util.{Collections, Properties}
import java.time.Duration
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

object KafkaJsonConsumer {

  def main(args: Array[String]): Unit = {

    val topics = "raj_test333.replica"
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
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "confluent-kafka.elisabeth.mytaxi.com:9092")

    //    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-cp-kafka-0.staging.otonomousmobility.com:9080")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "freenow_test_consumer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer")
    props.put("sasl.mechanism","PLAIN")
    props.put("security.protocol","SASL_PLAINTEXT")
    props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafkabroker\" password=\"KQ$3J.?3=Kk>:(ZS\";")

    //    props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor")
//    props.put("ssl.key.password","OhPiex3Aek5ooXee4Raiquie9ooth2eo")
//    props.put("ssl.keystore.location","/Users/raj.chinthalapellymytaxi.com/Downloads/kafka_mytaxi_staging_certs/kafka.client-mytaxi-staging-shared.keystore.jks")
//    props.put("ssl.keystore.password","OhPiex3Aek5ooXee4Raiquie9ooth2eo")
//    props.put("ssl.truststore.location","/Users/raj.chinthalapellymytaxi.com/Downloads/kafka_mytaxi_staging_certs/kafka.client-mytaxi-staging-shared.truststore.jks")
//    props.put("ssl.truststore.password","OhPiex3Aek5ooXee4Raiquie9ooth2eo")
//    props.put("security.protocol","SSL")
//    props.put("ssl.endpoint.identification.algorithm","")

    // props.put("schema.registry.url", "http://schemaregistry:8081")
//    props.put("ssl.key.password","ooMei8oojei2aiY9xua2ooboh8iech2j")
//    props.put("ssl.keystore.location","/Users/raj.chinthalapellymytaxi.com/Downloads/kafka_develop_mytaxi_certs/kafka.client-mytaxi-develop-shared.keystore.jks")
//    props.put("ssl.keystore.password","ooMei8oojei2aiY9xua2ooboh8iech2j")
//    props.put("ssl.truststore.location","/Users/raj.chinthalapellymytaxi.com/Downloads/kafka_develop_mytaxi_certs/kafka.client-mytaxi-develop-shared.truststore.jks")
//    props.put("ssl.truststore.password","ooMei8oojei2aiY9xua2ooboh8iech2j")

    props

  }

}
