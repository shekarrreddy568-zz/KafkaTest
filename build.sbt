name := "KafkaTest"

version := "0.1"

scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.0"

// https://mvnrepository.com/artifact/org.yaml/snakeyaml
libraryDependencies += "org.yaml" % "snakeyaml" % "1.24"

// https://mvnrepository.com/artifact/org.scala-lang/scala-reflect
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.12.8"

// https://mvnrepository.com/artifact/com.typesafe.play/play-json
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.3"

// https://mvnrepository.com/artifact/com.typesafe.play/play
libraryDependencies += "com.typesafe.play" %% "play" % "2.7.3"

// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams-scala
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.3.0"

libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"

libraryDependencies += "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.5"

resolvers += "io.confluent" at "http://packages.confluent.io/maven/"

libraryDependencies += "io.confluent" % "monitoring-interceptors" % "5.1.2"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"

// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.0.0-RC3"

libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "4.0.0"

libraryDependencies += "io.circe" %% "circe-yaml" % "0.10.0"

libraryDependencies += "io.confluent" % "kafka-streams-avro-serde" % "4.0.0"

libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies +=  "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "net.logstash.logback" % "logstash-logback-encoder" % "5.2"
