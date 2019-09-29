package com.lwq.spark.two_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object zipRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD1: RDD[Int] = sc.makeRDD(Array(1,2,3),2)
    val listRDD2: RDD[String] = sc.makeRDD(Array("a","b","c"),2)
    val zipRDD: RDD[(Int, String)] = listRDD1.zip(listRDD2)
    zipRDD.collect().foreach(println)
  }
}
