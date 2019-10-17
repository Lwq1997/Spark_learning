package com.lwq.spark.serialization

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object serTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("seriMethod")
    val context = new SparkContext(conf)

    val rdd: RDD[String] = context.makeRDD(Array("hello","lwq","spark","hive"))

    //3.创建一个Search对象
    val search = new Search()

    //4.运用第一个过滤函数并打印结果
    val match1: RDD[String] = search.getMatche2(rdd)
    match1.collect().foreach(println)
  }

}

class Search() {

  var query = "h"

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD（验证方法的序列化）
  def getMatche1 (rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD(验证属性的序列化)
  def getMatche2 (rdd: RDD[String]): RDD[String] = {
    val query: String = this.query
    rdd.filter(x => x.contains(query))
  }

}
