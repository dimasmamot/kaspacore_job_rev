package me.mamotis.spark.jobs.monitoring.stream.all_sensor.top_protocol

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

private[top_protocol] trait ProtocolStreamUtils {
  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(args: Array[String]): SparkSession ={
    val uri: String = args.headOption.getOrElse("mongodb://localhost/spark.protocolstream")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Protocol Count Stream to Mongo")
      .set("spark.app.id", "StreamProtocolCount2Mongo")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    MongoConnector(session.sparkContext).withDatabaseDo(WriteConfig(session), {db => db.drop()})
    session
  }
}
