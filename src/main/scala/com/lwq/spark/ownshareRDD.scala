package com.lwq.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}


object ownshareRDD {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[String] = sc.makeRDD(List("hive","hadoop","spark"),2)

    var sum = 0
    dataRDD.foreach(i=>sum+i)
    println(sum)


//    创建累加器
    val accumulator = new WordAccumulator

    //注册累加器
    sc.register(accumulator)

    dataRDD.foreach{
      case i => {
        accumulator.add(i)
      }

    }
    println("累加器的值： "+accumulator.value)
    //关闭连接
    sc.stop()
  }

}
class WordAccumulator extends AccumulatorV2[String,util.ArrayList[String]] {
  private val list = new util.ArrayList[String]()

  override def isZero: Boolean = list.isEmpty

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator
  }

  override def reset(): Unit = list.clear()

  override def add(v: String): Unit = {
    if(v.contains("h")){
      list.add(v)
    }
  }

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  override def value: util.ArrayList[String] = {
    list
  }
}