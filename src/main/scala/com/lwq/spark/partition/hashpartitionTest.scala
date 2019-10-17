package com.lwq.spark.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object hashpartitionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("cache").setMaster("local[*]")

    val context = new SparkContext(conf)

    context.setCheckpointDir("checkpoint")

   val rdd: RDD[(Int, Int)] = context.parallelize(List((1,3),(1,2),(2,4),(2,3),(3,6),(3,8)),8)

    rdd.mapPartitionsWithIndex((index,iter)=>{ Iterator(index.toString+" : "+iter.mkString("|")) }).collect.foreach(println)

    val hashpar = rdd.partitionBy(new HashPartitioner(7))

    println(hashpar.count)
    println(hashpar.partitioner)

    hashpar.mapPartitions(iter => Iterator(iter.length)).collect().foreach(println)
  }
}
