package com.lwq.spark.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
  */
object cogroupRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)


    val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd1 = sc.parallelize(Array((1,4),(2,5),(3,6)))

    rdd.cogroup(rdd1).collect().foreach(println)
  }
}
