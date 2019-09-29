package com.lwq.spark.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduceByKeyRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)


    val rdd = sc.parallelize(Array("one", "two", "two", "three", "three", "three"))
    val rdd2 = rdd.map((_,1))
     val reduceByKeyRDD: RDD[(String, Int)] = rdd2.reduceByKey(_+_)
    reduceByKeyRDD.collect().foreach(println)
  }
}