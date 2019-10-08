package com.lwq.spark.kv

import org.apache.spark.{SparkConf, SparkContext}

object foldByKeyRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)


    val rdd =  sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    rdd.glom().collect().foreach(array=>{
      println(array.mkString(","))
    })


    //初始值会给每个分区中的每一类key赋值一次
    rdd.foldByKey(0)(_+_).collect().foreach(println)
  }
}
