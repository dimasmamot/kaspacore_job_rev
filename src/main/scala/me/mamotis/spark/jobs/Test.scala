package me.mamotis.spark.jobs

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.col

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.datastax.spark.connector._

object Test extends CassandraUtils {
  def main(args: Array[String]): Unit = {
    val kafkaUrl = "localhost:9092"
    val schemaRegistryURL = "http://192.168.30.19:8081"
    val topic = "snoqttv4"

    val spark = getSparkSession(args)
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val rdd = spark.sparkContext.cassandraTable("test", "kv")
    println(rdd.count)
    println(rdd.first)
    println(rdd.map(_.getInt("value")).sum)

    val collection = spark.sparkContext.parallelize(Seq(("key3", 3), ("key4", 4)))
    collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))
  }
}
