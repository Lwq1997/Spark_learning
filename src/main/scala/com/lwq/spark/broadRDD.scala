package com.lwq.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}


object broadRDD {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(2,2),(3,3),(4,4)))
    val rdd2: RDD[(Int, Char)] = sc.makeRDD(List((1,'a'),(2,'b'),(3,'c'),(4,'d')))

    rdd1.join(rdd2).collect().foreach(println)

    val broad: Broadcast[List[(Int, Char)]] = sc.broadcast(List((1,'a'),(2,'b'),(3,'c'),(4,'d')))

    val rdd: RDD[(Int, (Int, Any))] = rdd1.map {
      case (k, v) => {
        var v2: Any = null
        for (t <- broad.value) {
          if (k == t._1) {
            v2 = t._2
          }
        }
        (k, (v, v2))
      }
    }
    rdd.collect().foreach(println)


    //关闭连接
    sc.stop()
  }

}