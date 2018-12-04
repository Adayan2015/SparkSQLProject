package com.fyy.spark
import org.apache.spark.sql.SparkSession

/**
  * @Title: SparkSessionApp
  * @ProjectName SparkSQLProject
  * @Description: TODO
  * @author fanyanyan
  * @date 2018/12/3 16:23
  */
/**
  * SparkSession的使用
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
    val people = spark.read.json("E:\\IDEA\\IdeaProject\\SparkSQLProject\\data\\people.json")
    people.show()

    spark.stop()


  }

}
