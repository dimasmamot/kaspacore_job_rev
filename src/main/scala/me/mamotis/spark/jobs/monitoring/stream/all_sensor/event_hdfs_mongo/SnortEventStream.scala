package me.mamotis.spark.jobs.monitoring.stream.all_sensor.event_hdfs_mongo

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_unixtime, to_utc_timestamp}
import org.apache.spark.sql.types.StringType
import org.bson._
import scala.collection.JavaConverters._
import scala.collection.mutable
import java.sql.Timestamp

object SnortEventStream extends EventStreamUtils {
  case class SignatureCountObj(signature: String, count: Long)
  case class ProtocolCountObj(protocol: String, count: Long)
  case class AlertMsgCountObj(alertMsg: String, count: Long)
  case class EventObj(timestamp: Timestamp,
                         device_id: String,
                         protocol: String,
                         ip_type: String,
                         src_mac: String,
                         dest_mac: String,
                         src_ip: String,
                         dest_ip: String,
                         src_port: Long,
                         dst_port: Long,
                         alert_msg: String,
                         classification: Long,
                         priority: Long,
                         sig_id: Long,
                         sig_gen: Long,
                         sig_rev: Long)

  def main(args: Array[String]): Unit = {
    val kafkaUrl = "localhost:9092"
    val schemaRegistryURL = "http://192.168.30.19:8081"
    val topic = "snoqttv4"

    val spark = getSparkSession(args)
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUrl)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    val utils = new ConfluentSparkAvroUtils(schemaRegistryURL)
    val keyDes = utils.deserializerForSubject(topic + "-key")
    val valDes = utils.deserializerForSubject(topic + "-value")

    val decoded = kafkaStreamDF.select(
      valDes(col("value")).alias("value")
    )

    val parsedRawDf = decoded.select("value.*")

    val eventDf = parsedRawDf.select(
        $"timestamp", $"device_id", $"protocol", $"ip_type", $"src_mac", $"dest_mac", $"src_ip",
        $"dest_ip", $"src_port", $"dst_port", $"alert_msg", $"classification", $"priority", $"sig_id",
        $"sig_gen", $"sig_rev")
      .map((r:Row) => EventObj(
        new Timestamp((r.getAs[String](0).toDouble * 1000).toLong),
        r.getAs[String](1),
        r.getAs[String](2),
        r.getAs[String](3),
        r.getAs[String](4),
        r.getAs[String](5),
        r.getAs[String](6),
        r.getAs[String](7),
        r.getAs[Long](8),
        r.getAs[Long](9),
        r.getAs[String](10),
        r.getAs[Long](11),
        r.getAs[Long](12),
        r.getAs[Long](13),
        r.getAs[Long](14),
        r.getAs[Long](15)
      ))

    val eventQuery = eventDf
      .writeStream
      .outputMode("append")
      .foreach(new ForeachWriter[EventObj]{

        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost/spark.eventsnort"))
        var mongoConnector: MongoConnector = _
        var eventMsg: mutable.ArrayBuffer[EventObj] = _

        override def open(partitionId: Long, version: Long): Boolean = {
          mongoConnector = MongoConnector(writeConfig.asOptions)
          eventMsg  = new mutable.ArrayBuffer[EventObj]()
          true
        }

        override def process(value: EventObj): Unit = {
          eventMsg.append(value)
        }

        override def close(errorOrNull: Throwable): Unit = {
          if (eventMsg.nonEmpty){
             mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
              collection.insertMany(eventMsg.map(em => {
                var doc = new Document()
                doc.put("timestamp", em.timestamp)
                doc.put("device_id", em.device_id)
                doc.put("protocol", em.protocol)
                doc.put("ip_type", em.ip_type)
                doc.put("src_mac", em.src_mac)
                doc.put("dest_mac", em.dest_mac)
                doc.put("src_ip", em.src_ip)
                doc.put("dest_ip", em.dest_ip)
                doc.put("src_port", em.src_port)
                doc.put("dst_port", em.dst_port)
                doc.put("alert_msg", em.alert_msg)
                doc.put("classification", em.classification)
                doc.put("priority", em.priority)
                doc.put("sig_id", em.sig_id)
                doc.put("sig_gen", em.sig_gen)
                doc.put("sig_rev", em.sig_rev)
                doc
              }).asJava)
            })
          }
        }
      }).start().awaitTermination()
  }
}
