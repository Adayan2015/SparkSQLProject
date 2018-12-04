package com.fyy.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @Title: SQLContextApp
  * @ProjectName SparkSQLProject
  * @Description: TODO
  * @author fanyanyan
  * @date 2018/12/3 14:30
  */

/**
  * SQLContext的使用
  * 注意：IDEA是在本地，而测试数据是在服务器上
  */
object SQLContextApp {
  def main(args: Array[String]): Unit ={

    val path = args(0)

    // 1)创建相应的Context
    val sparkConf = new SparkConf()

    // 在测试或者生产中，AppName和Master我们是通过脚本进行指定
//    sparkConf.setAppName("SQLContextAPP").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // 2)相应的处理
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    // 3)关闭资源
    sc.stop()

  }

}
