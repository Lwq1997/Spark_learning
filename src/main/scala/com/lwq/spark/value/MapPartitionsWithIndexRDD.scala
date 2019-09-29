package com.lwq.spark.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Lwq
  * @Date: 2019/9/28 19:08
  * @Version 1.0
  * @Describe
  */
object MapPartitionsWithIndexRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //mappartitions算子，是对一个RDD中所有的分区进行遍历，分区有几个就 执行几次。速度比map快，但是会出现OOM
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,2)
    val mapPartitionsWithIndexRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号:" + num))
      }
    }

    mapPartitionsWithIndexRDD.collect().foreach(println)
  }
}
