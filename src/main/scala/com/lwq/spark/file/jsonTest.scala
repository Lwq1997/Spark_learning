package com.lwq.spark.file

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object jsonTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("cache").setMaster("local[*]")

    val context = new SparkContext(conf)

    val jsonRDD: RDD[String] = context.textFile("file/user.json")

    jsonRDD.map(JSON.parseFull).collect().foreach(println)
  }
}
