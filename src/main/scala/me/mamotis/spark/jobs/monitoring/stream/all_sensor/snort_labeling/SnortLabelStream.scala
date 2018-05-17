package me.mamotis.spark.jobs.monitoring.stream.all_sensor.snort_labeling

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.bson._

import scala.collection.JavaConverters._
import scala.collection.mutable

object SnortLabelStream extends LabelStreamUtils {
  case class SignatureCountObj(signature: String, count: Long)
  case class ProtocolCountObj(protocol: String, count: Long)
  case class AlertMsgCountObj(alertMsg: String, count: Long)
  case class LabelMsgObj(timestamp: Double,
                         dest_ip: String,
                         dst_port: Long,
                         src_port: Long,
                         src_ip: String,
                         alert_msg: String)

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
      .option("startingOffsets", "earliest")
      .load()

    val utils = new ConfluentSparkAvroUtils(schemaRegistryURL)
    val keyDes = utils.deserializerForSubject(topic + "-key")
    val valDes = utils.deserializerForSubject(topic + "-value")

    val decoded = kafkaStreamDF.select(
      valDes(col("value")).alias("value")
    )

    val parsedRawDf = decoded.select("value.*")

    val labelDf = parsedRawDf.select(
        $"timestamp", $"dest_ip", $"dst_port", $"src_port", $"src_ip",$"alert_msg")
      .map((r:Row) => LabelMsgObj(
        r.getAs[String](0).toDouble,
        r.getAs[String](1),
        r.getAs[Long](2),
        r.getAs[Long](3),
        r.getAs[String](4),
        r.getAs[String](5)
      ))

    val labelQuery = labelDf
      .writeStream
      .outputMode("append")
      .foreach(new ForeachWriter[LabelMsgObj]{

        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost/spark.labelsnort"))
        var mongoConnector: MongoConnector = _
        var LabelMsg: mutable.ArrayBuffer[LabelMsgObj] = _

        override def open(partitionId: Long, version: Long): Boolean = {
          mongoConnector = MongoConnector(writeConfig.asOptions)
          LabelMsg  = new mutable.ArrayBuffer[LabelMsgObj]()
          true
        }

        override def process(value: LabelMsgObj): Unit = {
          LabelMsg.append(value)
        }

        override def close(errorOrNull: Throwable): Unit = {
          if (LabelMsg.nonEmpty){
            mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
              collection.insertMany(LabelMsg.map(lm => {
                var doc = new Document()
                doc.put("ts_snort", lm.timestamp)
                doc.put("dst_ip", lm.dest_ip)
                doc.put("dst_port", lm.dst_port)
                doc.put("src_port", lm.src_port)
                doc.put("src_ip", lm.src_ip)
                doc.put("label",lm.alert_msg)
                doc
              }).asJava)
            })
          }
        }
      }).start()

//    val sigDf = parsedRawDf.groupBy("sig_id").count()
//      .map((r:Row) => SignatureCountObj(r.getAs[Long](0).toString, r.getAs[Long](1)))
//
//    val alertmsgDf = parsedRawDf.groupBy("alert_msg").count()
//      .map((r:Row) => AlertMsgCountObj(r.getAs[String](0), r.getAs[Long](1)))
//
//    val protocolDf = parsedRawDf.groupBy("protocol").count()
//      .map((r:Row) => ProtocolCountObj(r.getAs[String](0), r.getAs[Long](1)))

    // Sink to mongod , db spark, collection sigcount
//    val signatureCountQuery = sigDf
//      .writeStream
//      .outputMode("complete")
//      .foreach(new ForeachWriter[SignatureCountObj] {
//
//        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost/spark.sigcount"))
//        var mongoConnector: MongoConnector = _
//        var signatureCounts: mutable.ArrayBuffer[SignatureCountObj] = _
//
//        override def process(value: SignatureCountObj): Unit = {
//          signatureCounts.append(value)
//        }
//
//        override def close(errorOrNull: Throwable): Unit = {
//          if(signatureCounts.nonEmpty){
//            mongoConnector.withCollectionDo(writeConfig, {collection: MongoCollection[Document] =>
//              collection.insertMany(signatureCounts.map(sc => {new Document(sc.signature, sc.count)}).asJava)
//            })
//          }
//        }
//
//        override def open(partitionId: Long, version: Long): Boolean = {
//          mongoConnector = MongoConnector(writeConfig.asOptions)
//          signatureCounts = new mutable.ArrayBuffer[SignatureCountObj]()
//          true
//        }
//      }).start()

    // Sink to mongod, db spark, collection msgcount
//    val msgCountQuery = alertmsgDf
//      .writeStream
//      .outputMode("complete")
//      .foreach(new ForeachWriter[AlertMsgCountObj] {
//
//        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost/spark/msgcount"))
//        var mongoConnector: MongoConnector = _
//        var alertMsgCounts: mutable.ArrayBuffer[AlertMsgCountObj] = _
//
//        override def process(value: AlertMsgCountObj): Unit = {
//          alertMsgCounts.append(value)
//        }
//
//        override def close(errorOrNull: Throwable): Unit = {
//          if(alertMsgCounts.nonEmpty){
//            mongoConnector.withCollectionDo(writeConfig, {collection: MongoCollection[Document] =>
//              collection.insertMany(alertMsgCounts.map(amc => {new Document(amc.alertMsg, amc.count)}).asJava)
//            })
//          }
//        }
//
//        override def open(partitionId: Long, version: Long): Boolean = {
//          mongoConnector = MongoConnector(writeConfig.asOptions)
//          alertMsgCounts = new mutable.ArrayBuffer[AlertMsgCountObj]()
//          true
//        }
//      })

    // Sink to mongod, db spark, collection protocolcount
//    val protocolCountQuery = protocolDf
//      .writeStream
//      .outputMode("complete")
//      .foreach(new ForeachWriter[ProtocolCountObj] {
//
//        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost/spark.protocolcount"))
//        var mongoConnector: MongoConnector = _
//        var protocolCounts: mutable.ArrayBuffer[ProtocolCountObj] = _
//
//        override def process(value: ProtocolCountObj): Unit = {
//          protocolCounts.append(value)
//        }
//
//        override def close(errorOrNull: Throwable): Unit = {
//          if(protocolCounts.nonEmpty){
//            mongoConnector.withCollectionDo(writeConfig, {collection: MongoCollection[Document] =>
//              collection.insertMany(protocolCounts.map(pc => {new Document(pc.protocol, pc.count)}).asJava)
//            })
//          }
//        }
//
//        override def open(partitionId: Long, version: Long): Boolean = {
//          mongoConnector = MongoConnector(writeConfig.asOptions)
//          protocolCounts = new mutable.ArrayBuffer[ProtocolCountObj]()
//          true
//        }
//      }).start()

    // Sink to HDFS as immutable raw data
//    val parsedRawToHDFSQuery = parsedRawDf
//      .writeStream
//      .option("checkpointLocation", "hdfs://localhost:9000/checkpoint/stream/snort")
//      .option("path","hdfs://localhost:9000/input/spark/stream/snort")
//      .outputMode("append")
//      .format("json")
//      .start()

//    signatureCountQuery.awaitTermination()
//    msgCountQuery.awaitTermination()
//    parsedRawToHDFSQuery.awaitTermination()
//    protocolCountQuery.awaitTermination()
    labelQuery.awaitTermination()
  }
}
