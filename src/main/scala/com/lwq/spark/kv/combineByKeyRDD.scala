package com.lwq.spark.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object combineByKeyRDD {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(conf)


    val rdd =  sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
    rdd.glom().collect().foreach(array=>{
      println(array.mkString(","))
    })


    //初始值会给每个分区中的每一类key赋值一次
    val comRDD: RDD[(String, (Int, Int))] = rdd.combineByKey((_, 1)
      , (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1)
      , (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    comRDD.collect().foreach(println)

//   分区内： 88--->(88,1)--->(88,1),91--->(179,2)

    comRDD.map{case (key,value)=>(key,value._1/value._2.toDouble)}.collect().foreach(println)

  }
}
