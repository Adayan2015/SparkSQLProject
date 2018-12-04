package com.fyy.spark

import org.apache.spark.sql.SparkSession

/**
  * @Title: DatasetApp
  * @ProjectName SparkSQLProject
  * @Description: TODO
  * @author fanyanyan
  * @date 2018/12/4 18:08
  */
object DatasetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()

    // 注意：需要导入隐式转换
    import spark.implicits._

    val path = "E:\\IDEA\\IdeaProject\\SparkSQLProject\\data\\sales.csv"

    // spark如何解析csv文件
    val df = spark.read.option("header","true").option("inferSchema", "true").csv(path)
    df.show()

    val ds = df.as[Sales]
    ds.map(line => line.itemId).show()

  }

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)
}
