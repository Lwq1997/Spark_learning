package com.lwq.sparkstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object fileWordCount {
  def main(args: Array[String]): Unit = {
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("fileWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.监控文件夹创建DStream
    val dirStream = ssc.textFileStream("file:///Users/lwq/IdeaProjects/Spark_learning/sparkstreamfile")

    //4.将每一行数据做切分，形成一个个单词
    val wordStreams = dirStream.flatMap(_.split(","))

    //5.将单词映射成元组（word,1）
    val wordAndOneStreams = wordStreams.map((_, 1))

    //6.将相同的单词次数做统计
    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_ + _)

    //7.打印
    wordAndCountStreams.print()

    //8.启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()


  }
}
