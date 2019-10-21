package com.lwq.sparksql

import javax.xml.ws.ServiceMode
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object JDBCSqlTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]")


    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val jdbcDF: DataFrame = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/rdd")
      .option("dbtable", "user")
      .option("user", "root")
      .option("password", "123456")
      .load()
    jdbcDF.show()

    //往mysql写数据
    import spark.implicits._
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(Array((1,"lll",23),(2,"www",32),(3,"qqq",44)))
    val jdbcDF1: DataFrame = rdd.toDF("id","name","age")

    jdbcDF1.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/rdd")
      .option("dbtable", "user")
      .option("user", "root")
      .option("password", "123456")
      .mode(SaveMode.Append)
      .save()

  }
}
