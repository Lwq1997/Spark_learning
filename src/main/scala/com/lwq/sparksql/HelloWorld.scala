package com.lwq.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}


object HelloWorld {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkconf对象
    //设定spark计算框架的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val df = spark.read.json("file/user.json")


    val peopleDF = spark.read.format("json").load("file/user.json")
    //peopleDF.write.format("parquet").save("file/user.parquet")

    val peopleParquetDF: DataFrame = spark.read.parquet("file/user.parquet")
    peopleParquetDF.createOrReplaceTempView("peopleParquet")
    val nameDF: DataFrame = spark.sql("select name from peopleParquet")
    nameDF.show()

    import spark.implicits._

    df.show()

    df.filter($"age" > 21).show()

    df.createOrReplaceTempView("users")

    spark.sql("SELECT * FROM users where age = 20").show()


    spark.udf.register("addName", (x: String) => "Name:" + x)

    spark.sql("SELECT addName(name),age FROM users where age = 20").show()

    // 注册函数
    val average = new MyAverage
    spark.udf.register("myAverage", average)

    spark.sql("SELECT myAverage(age) as average_age FROM users").show()

    val averageAge = new MyAverage01().toColumn.name("average_age")
    val result = df.as[User].select(averageAge)
    result.show()

    spark.stop()

  }
}


class MyAverage extends UserDefinedAggregateFunction {
  // 聚合函数输入参数的数据类型
  def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

  // 聚合缓冲区中值得数据类型
  def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  // 返回值的数据类型
  def dataType: DataType = DoubleType

  // 对于相同的输入是否一直返回相同的输出。
  def deterministic: Boolean = true

  // 初始化
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 存工资的总额
    buffer(0) = 0L
    // 存工资的个数
    buffer(1) = 0L
  }

  // 相同Execute间的数据合并。
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 不同Execute间的数据合并
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终结果
  def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
}



case class User(name: String, age: Long)
case class Average(var sum: Long, var count: Long)


class MyAverage01 extends Aggregator[User, Average, Double] {
  // 定义一个数据结构，保存工资总数和工资总个数，初始都为0
  def zero: Average = Average(0L, 0L)
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Average, user: User): Average = {
    buffer.sum += user.age
    buffer.count += 1
    buffer
  }
  // 聚合不同execute的结果
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }
  // 计算输出
  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
  // 设定之间值类型的编码器，要转换成case类
  // Encoders.product是进行scala元组和case类转换的编码器
  def bufferEncoder: Encoder[Average] = Encoders.product
  // 设定最终输出值的编码器
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

