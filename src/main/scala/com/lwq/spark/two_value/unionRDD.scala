package com.lwq.spark.two_value

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object unionRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD1: RDD[Int] = sc.makeRDD(1 to 10)
    val listRDD2: RDD[Int] = sc.makeRDD(1 to 10)
    val unionRDD: RDD[Int] = listRDD1.union(listRDD2)
    unionRDD.collect().foreach(println)
  }
}
