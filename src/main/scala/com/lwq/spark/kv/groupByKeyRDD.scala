package com.lwq.spark.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object groupByKeyRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)


    val rdd = sc.parallelize(Array("one", "two", "two", "three", "three", "three"))
    val rdd2 = rdd.map((_,1))
    val groupbykeyRDD: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    groupbykeyRDD.collect().foreach(println)
    groupbykeyRDD.map(t => (t._1, t._2.sum)).collect().foreach(println)
  }
}