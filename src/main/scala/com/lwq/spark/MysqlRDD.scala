package com.lwq.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object MysqlRDD {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/rdd"
    val userName = "root"
    val passWd = "123456"

    /*
    //查询数据
     val rdd: JdbcRDD[(Int, String)] = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from `user` where `id`>=? and `id`<=?;",
      1,
      3,
      1,
      r => (r.getInt(1), r.getString(2))
    )


    //打印最后结果
    println(rdd.count())
    rdd.foreach(println)
 */
    //    插入数据1
    /*
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)

        val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("zhangsan", 20), ("lisi", 30), ("wangwu", 40)))
        rdd.foreach {
          case (name, age) => {
            var sql = "insert into user (name,age) values(?,?)"
            val statement: PreparedStatement = connection.prepareStatement(sql)
            statement.setString(1, name)
            statement.setInt(2, age)
            statement.executeUpdate()
            statement.close()
          }

        }
    */

    //    插入数据2


    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("zhangsan", 20), ("lisi", 30), ("wangwu", 40)))

    rdd.foreachPartition(datas => {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)

      datas.foreach {
        case (name, age) => {
          var sql = "insert into user (name,age) values(?,?)"
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setString(1, name)
          statement.setInt(2, age)
          statement.executeUpdate()
          statement.close()
        }
      }

      connection.close()
    })

    sc.stop()

  }


  //  def main(args: Array[String]) {
  //    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseApp")
  //    val sc = new SparkContext(sparkConf)
  //    val data = sc.parallelize(List("Female", "Male","Female"))
  //
  //    data.foreachPartition(insertData)
  //  }
  //
  //  def insertData(iterator: Iterator[String]): Unit = {
  //    Class.forName ("com.mysql.jdbc.Driver").newInstance()
  //    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://hadoop102:3306/rdd", "root", "000000")
  //    iterator.foreach(data => {
  //      val ps = conn.prepareStatement("insert into rddtable(name) values (?)")
  //      ps.setString(1, data)
  //      ps.executeUpdate()
  //    })
  //  }

}
