package com.lwq.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Lwq
  * @Date: 2019/9/28 18:23
  * @Version 1.0
  * @Describe
  */
object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //从集合创建
    //makeRDD底层调用parallelize
    val listRDD = sc.makeRDD(List(1,2,3,4))
    listRDD.collect().foreach(println)

    val arrayRDD: RDD[Int] = sc.parallelize(Array(1,2,3,4))
     arrayRDD.collect().foreach(println)

    //从外部的开发环境创建,分区数是按最小的分区数，但是不一定是这个数，他依赖于hadoop的文件分片规则
    val fileRDD: RDD[String] = sc.textFile("in")
    fileRDD.collect().foreach(println)

    listRDD.saveAsTextFile("output")
    fileRDD.saveAsTextFile("output1")

  }
}
