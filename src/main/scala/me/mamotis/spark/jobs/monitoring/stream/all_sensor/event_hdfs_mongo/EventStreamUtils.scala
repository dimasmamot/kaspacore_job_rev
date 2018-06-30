package me.mamotis.spark.jobs.monitoring.stream.all_sensor.event_hdfs_mongo

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

private[event_hdfs_mongo] trait EventStreamUtils {
  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val uri: String = args.headOption.getOrElse("mongodb://localhost/spark.eventsnort")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Snort Labeling Stream to Mongo")
      .set("spark.app.id", "SnortLabeling2Mongo")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    MongoConnector(session.sparkContext).withDatabaseDo(WriteConfig(session), {db => db.drop()})
    session
  }
}
