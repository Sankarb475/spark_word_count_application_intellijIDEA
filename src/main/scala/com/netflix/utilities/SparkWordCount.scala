package com.netflix.utilities

import org.apache.spark.sql.SparkSession

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._

object SparkWordCount {
  def main(args: Array[String]) {

    // create Spark context with Spark configuration
    println("starting")

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("dummy").set("spark.driver.allowMultipleContexts", "false").set("spark.ui.enabled", "false")

    val sc = new SparkContext(conf)

    // get threshold
    //val threshold = args(1).toInt
    //print
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._


    // read in text file and split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    println(tokenized.toDF.show(false))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    println(wordCounts.toDF.show)
    // filter out words with fewer than threshold occurrences
    //val filtered = wordCounts.filter(_._2 >= threshold)

    // count characters
    //val charCounts = wordCounts.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    //System.out.println(charCounts.collect().mkString(", "))
  }
}


