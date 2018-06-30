name := "KaspaCoreRevamped"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "confluent" at "http://packages.confluent.io/maven/"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.1"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.1"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"

libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.1"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"

libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "4.0.0"

libraryDependencies += "com.databricks" % "spark-avro_2.11" % "4.0.0"

libraryDependencies += "org.scalaz" % "scalaz-core_2.11" % "7.3.0-M21"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-kms" % "1.11.313"

libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.2.2"

libraryDependencies += "joda-time" % "joda-time" % "2.10"

libraryDependencies += "datastax" % "spark-cassandra-connector" % "2.0.8-s_2.11"
