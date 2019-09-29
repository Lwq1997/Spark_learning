package com.lwq.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Lwq
  * @Date: 2019/9/22 19:13
  * @Version 1.0
  * @Describe
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //读取文件
    val line = sc.textFile("in")

    //将文件内容扁平化
    val words = line.flatMap(_.split(" "))

    //将string转换为map
    val wordToOne = words.map((_,1))

    //将转换结构后的数据进行分组聚合
    val wordToSum = wordToOne.reduceByKey(_+_)

    //将统计结果采集后打印
    val result = wordToSum.collect()

    result.foreach(println)
    //    println(sc)

    sc.stop()
  }
}

