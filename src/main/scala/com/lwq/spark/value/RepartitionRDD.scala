package com.lwq.spark.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Lwq
  * @Date: 2019/9/28 19:08
  * @Version 1.0
  * @Describe  所有算子的计算功能通过executor执行（_*2）
  */
object RepartitionRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16,4)
    val repartitionRDD: RDD[Int] = listRDD.repartition(2 )
    repartitionRDD.collect().foreach(println)
    repartitionRDD.glom().collect().foreach(array=>{
      println(array.mkString(","))
    })
  }
}
