package com.lwq.spark.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD
  */
object sortByKeyRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)


    val rdd =   sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    rdd.glom().collect().foreach(array=>{
      println(array.mkString(","))
    })


    //初始值会给每个分区中的每一类key赋值一次
    rdd.sortByKey(true).collect().foreach(println)
    rdd.sortByKey(false).collect().foreach(println)
  }
}
