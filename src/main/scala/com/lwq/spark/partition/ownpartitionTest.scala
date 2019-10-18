package com.lwq.spark.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object ownpartitionTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("cache").setMaster("local[*]")

    val context = new SparkContext(conf)

    context.setCheckpointDir("checkpoint")

   val rdd: RDD[(Int, Int)] = context.parallelize(Array((1,1),(2,2),(3,3),(4,4),(5,5),(6,6)))

    //没有分区器
    rdd.mapPartitionsWithIndex((index,iter)=>{ Iterator(index.toString+" : "+iter.mkString("|")) }).collect.foreach(println)

    val ownpart = rdd.partitionBy(new CustomerPartitioner(2))

    println(ownpart.count)
    println(ownpart.partitioner)
    //使用自定义分区器
    println("使用自定义分区器")
    ownpart.mapPartitionsWithIndex((index,iter)=>{ Iterator(index.toString+" : "+iter.mkString("|")) }).collect().foreach(println)

    ownpart.mapPartitions(iter => Iterator(iter.length)).collect().foreach(println)
  }
}
class CustomerPartitioner(numParts:Int) extends  org.apache.spark.Partitioner{

  //覆盖分区数
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val ckey: String = key.toString
    ckey.substring(ckey.length-1).toInt%numParts
  }
}