package com.lwq.spark.file

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object seqTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("cache").setMaster("local[*]")

    val context = new SparkContext(conf)

   val rdd: RDD[(Int, Int)] = context.makeRDD(Array((1,2),(3,4),(5,6)))

//    rdd.saveAsSequenceFile("file/seq")

    val rdd1: RDD[(Int, Int)] = context.sequenceFile[Int, Int]("file/seq")
    rdd1.collect().foreach(println)
  }
}
