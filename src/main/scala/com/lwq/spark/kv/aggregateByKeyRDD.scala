package com.lwq.spark.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
分区内找到key相同的对应的最大value值，然后相加
 */
object aggregateByKeyRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)


    val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    rdd.glom().collect().foreach(array=>{
      println(array.mkString(","))
    })


    //初始值会给每个分区中的每一类key赋值一次
    rdd.aggregateByKey(0)(Math.max(_,_),_+_).collect().foreach(println)
    rdd.aggregateByKey(10)(Math.max(_,_),_+_).collect().foreach(println)
    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)//wordcount
  }
}