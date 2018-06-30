package me.mamotis.spark.jobs.monitoring.stream.corresponding_sensor.snort_labeling

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

private[spark] trait StreamUtils {
  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val uri: String = args.headOption.getOrElse("mongodb://10.252.108.99/spark.labelsnort")

    val conf = new SparkConf()
      .setMaster("spark://master.pens.ac.id:7077")
      .setAppName("StreamProtocolCountToMongo")
      .set("spark.app.id", "StreamProtocolCountToMongo")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    MongoConnector(session.sparkContext).withDatabaseDo(WriteConfig(session), {db => db.drop()})
    session
  }
}
