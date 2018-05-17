package me.mamotis.spark.jobs.monitoring.stream.all_sensor.top_protocol

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.bson._

import scala.collection.JavaConverters._
import scala.collection.mutable

class TopProtocolStream extends ProtocolStreamUtils {
  case class ProtocolCountObj(protocol: String, count: Long)

  def main(args: Array[String]): Unit = {
    val kafkaUrl = "localhost:9092"
    val schemaRegistryURL = "http://192.168.30.19:8081"
    val topic = "snoqttv4"

    val spark = getSparkSession(args)
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val kafkastreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUrl)
      .option("subscribe", topic)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val utils = new ConfluentSparkAvroUtils(schemaRegistryURL)
    val keyDes = utils.deserializerForSubject(topic + "-key")
    val valDes = utils.deserializerForSubject(topic + "-value")

    val decoded = kafkastreamDF.select(
      valDes(col("value")).alias("value")
    )

    val parsedRawDf = decoded.select("value.*")

    val protocolDf = parsedRawDf.groupBy("protocol").count()
      .map((r:Row) =>
        ProtocolCountObj(r.getAs[String](0), r.getAs[Long](1)))
  }

}
