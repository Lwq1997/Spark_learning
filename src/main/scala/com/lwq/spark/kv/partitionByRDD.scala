package com.lwq.spark.kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object partitionByRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)


    val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
    println(rdd.partitions.size)
    var rdd2 = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))
    val mapwithindex: RDD[((Int, String), String)] = rdd2.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号:" + num))
      }
    }
    mapwithindex.collect().foreach(println)
  }
}
