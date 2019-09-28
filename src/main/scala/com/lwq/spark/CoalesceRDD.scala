package com.lwq.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Lwq
  * @Date: 2019/9/28 19:08
  * @Version 1.0
  * @Describe  Coalesce只能传一个比该算子前数据分区数小的一个数字，比如3分区合并为2分区，就是对1分区的数据不动，然后将原2,3分区的数弄到2分区
  */
object CoalesceRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)

    //缩减分区数
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,3)
    println("缩减分区前："+listRDD.partitions.size)
    val coalesceRDD: RDD[Int] = listRDD.coalesce(2)
    println("缩减分区后："+coalesceRDD.partitions.size)

  }
}
