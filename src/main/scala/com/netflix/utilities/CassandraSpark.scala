package com.netflix.utilities
import com.datastax.driver.core.{ResultSet, Row}
//import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object CassandraSpark {

    def main(args: Array[String]) {

      val a = args(0).toInt
      val b = args(1).toInt
      val conf = new SparkConf().setAppName("APP_NAME")
        .setMaster("local")
        .set("spark.cassandra.connection.host", "localhost")
        .set("spark.cassandra.auth.username", "")
        .set("spark.cassandra.auth.password", "")

      println(conf)
      val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

      val sc: SparkContext = spark.sparkContext

      println("before")
      val data = sc.cassandraTable("datatable", "data")


      println(data.collect.foreach(println))


  }
}
