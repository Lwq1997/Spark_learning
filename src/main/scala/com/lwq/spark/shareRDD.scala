package com.lwq.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}


object shareRDD {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    var sum = 0
    dataRDD.foreach(i=>sum+i)
    println(sum)

//    创建累加器
    val accumulator: LongAccumulator = sc.longAccumulator

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