package me.mamotis.spark.jobs

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

private[jobs] trait CassandraUtils{

  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(strings: Array[String]): SparkSession = {
    val conf = new SparkConf(true)
      .setMaster("local[*]")
      .setAppName("Raw Data Stream to Cassandra")
      .set("spark.app.id", "CassandraRawData Stream")
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    session
  }

}
