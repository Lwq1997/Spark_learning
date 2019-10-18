package com.lwq.spark.file

import org.apache.spark.{SparkConf, SparkContext}


object objectTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("cache").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(1,2,3,4))

    rdd.saveAsObjectFile("file/object")

    val objFile = sc.objectFile[Int]("file/object")

    objFile.collect().foreach(println)
  }
}
