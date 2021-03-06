package com.lwq.spark.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Lwq
  * @Date: 2019/9/28 19:08
  * @Version 1.0
  * @Describe
  */
object SortByRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(List(2,13,4,23,14))
    val sortByRDD: RDD[Int] = listRDD.sortBy(x=>x,false)
    val sortByRDD1: RDD[Int] = listRDD.sortBy(x=>x%3,false)
    sortByRDD.collect().foreach(println)
    sortByRDD1.collect().foreach(println)
  }
}
