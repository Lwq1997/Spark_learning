package com.lwq.spark.partition

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner


object partitionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("cache").setMaster("local[*]")

    val context = new SparkContext(conf)

    context.setCheckpointDir("checkpoint")

   val rdd: RDD[(Int, Int)] = context.parallelize(List((1,1),(2,2),(3,3)))


    println(rdd.partitioner)

    val partitioned = rdd.partitionBy(new HashPartitioner(2))

    println(rdd.partitioner)
  }
}
