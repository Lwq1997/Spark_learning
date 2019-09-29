package com.lwq.spark.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Lwq
  * @Date: 2019/9/28 19:08
  * @Version 1.0
  * @Describe  Distinct会有shuffle
  */
object DistinctRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD: RDD[Int] = sc.makeRDD(List(1,1,2,3,3,4,1,2,3,4,5,6,7,8,9))
    val distinctRDD: RDD[Int] = listRDD.distinct()
    //结果放在两个分区中
//    val distinctRDD: RDD[Int] = listRDD.distinct(2)
    distinctRDD.collect().foreach(println)
  }
}
