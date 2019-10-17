package com.lwq.spark.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object cacheTast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("cache").setMaster("local[*]")

    val context = new SparkContext(conf)

    val rdd: RDD[Int] = context.makeRDD(Array(0,1,2,3,4,5))

    val nocache = rdd.map(_.toString+System.currentTimeMillis).cache()


    nocache.collect()
  }

}
