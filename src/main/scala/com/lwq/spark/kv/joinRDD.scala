package com.lwq.spark.kv

import org.apache.spark.{SparkConf, SparkContext}

object joinRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)


    val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd1 = sc.parallelize(Array((1,4),(2,5),(3,6)))
    val rdd2 = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc")))

    rdd.join(rdd1).collect().foreach(println)
    rdd.join(rdd1).join(rdd2).collect().foreach(println)
  }
}
