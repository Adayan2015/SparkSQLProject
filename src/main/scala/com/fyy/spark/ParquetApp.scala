package com.fyy.spark

import org.apache.spark.sql.SparkSession

/**
  * @Title: ParquetApp
  * @ProjectName SparkSQLProject
  * @Description: TODO
  * @author fanyanyan
  * @date 2018/12/6 17:18
  */

/**
  * Parquet文件操作
  */
object ParquetApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()

    // val userDF = spark.read.format("parquet").load("E:\\IDEA\\IdeaProject\\SparkSQLProject\\data\\users.parquet")
    /**
      * spark.read.format("parquet").load 这是标准写法
      */
    val userDF = spark.read.format("parquet").load("file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")

    userDF.printSchema()
    userDF.show()
    userDF.select("name", "favorite_color").show()
    userDF.select("name", "favorite_color").write.format("json").save("file:///home/hadoop/tmp/jsonout")

    // sparksql默认处理的format就是parquet
    spark.read.load("file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")
    // 会报错
    spark.read.load("file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/person.json")

    spark.read.format("parquet").option("path","file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").load().show

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "10")

    spark.sqlContext.getConf("spark.sql.shuffle.partitions")

  }

}
