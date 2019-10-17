package com.lwq.spark.cache

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object checkpointTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("cache").setMaster("local[*]")

    val context = new SparkContext(conf)

    context.setCheckpointDir("checkpoint")

    val rdd: RDD[Int] = context.makeRDD(Array(0,1,2,3,4,5))

    val nocache = rdd.map(_.toString+System.currentTimeMillis)


    nocache.checkpoint()

    nocache.collect().foreach(println)
  }
}
