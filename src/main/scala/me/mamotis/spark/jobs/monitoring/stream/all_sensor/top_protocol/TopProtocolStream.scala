package me.mamotis.spark.jobs.monitoring.stream.all_sensor.top_protocol

import java.sql.Timestamp

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_unixtime, lit, to_utc_timestamp, window}
import org.bson._
import org.apache.spark.sql.types.StringType
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.JavaConverters._
import scala.collection.mutable

object TopProtocolStream extends ProtocolStreamUtils {
  case class ProtocolCountObj(protocol: String, datetime: Timestamp , count: Long)

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
      .option("startingOffsets", "latest")
      .load()

    val utils = new ConfluentSparkAvroUtils(schemaRegistryURL)
    val keyDes = utils.deserializerForSubject(topic + "-key")
    val valDes = utils.deserializerForSubject(topic + "-value")

    val decoded = kafkastreamDF.select(
      valDes(col("value")).alias("value")
    )

    val parsedRawDf = decoded.select("value.*")

    val protocolDf = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"),"GMT").alias("timestamp").cast(StringType), $"protocol")
      .withColumn("value", lit(1))

    val windowedDf = protocolDf.groupBy(
      $"protocol",
      window($"timestamp", "1 seconds").alias("window")
    ).sum("value")

//    windowedDf.printSchema()


    val proCountDf = windowedDf.map(
      (r:Row) => ProtocolCountObj(
        r.getAs[String](0),
        r.getStruct(1).getAs[Timestamp](0),
        r.getAs[Long](2)
      )
    )

    proCountDf.printSchema()

    val protocolQuery = windowedDf
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
      .start()
      .awaitTermination()

//    val protocolQuery = proCountDf
//      .writeStream
//      .outputMode("update")
//      .foreach(new ForeachWriter[ProtocolCountObj]{
//
//        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost/spark.protocolstream"))
//        var mongoConnector: MongoConnector = _
//        var ProtocolCount: mutable.ArrayBuffer[ProtocolCountObj] = _
//
//        override def open(partitionId: Long, version: Long): Boolean = {
//          mongoConnector = MongoConnector(writeConfig.asOptions)
//          ProtocolCount = new mutable.ArrayBuffer[ProtocolCountObj]()
//          true
//        }
//
//        override def process(value: ProtocolCountObj): Unit = {
//          ProtocolCount.append(value)
//        }
//
//        override def close(errorOrNull: Throwable): Unit = {
//          if (ProtocolCount.nonEmpty){
//            ProtocolCount.map(
//              pc => {
//                println(pc.datetime.getTime())
//                var curDate:DateTime = new DateTime(pc.datetime.getTime()/1000, DateTimeZone.UTC)
//                println("Year : " + curDate.getYear())
//                println("Month : " + curDate.getMonthOfYear())
//                println("Day : " + curDate.getDayOfMonth())
//                println("Hour : " + curDate.getHourOfDay())
//                println("Minute : " + curDate.getMinuteOfHour())
//                println("Second : " + curDate.getSecondOfMinute())
//                println("-----------------")
//              }
//            )
//          }
//        }
//      }).start().awaitTermination()
  }

}
