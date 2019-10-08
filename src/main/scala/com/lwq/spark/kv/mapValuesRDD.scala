package com.lwq.spark.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
  *  针对于(K,V)形式的类型只对V进行操作
  */
object mapValuesRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)


    val rdd =  sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    rdd.glom().collect().foreach(array=>{
      println(array.mkString(","))
    })


    //初始值会给每个分区中的每一类key赋值一次
    rdd.mapValues(_+"|||").collect().foreach(println)
  }
}
