package com.fyy.spark

import org.apache.spark.sql.SparkSession

/**
  * @Title: DataFrameApp
  * @ProjectName SparkSQLProject
  * @Description: TODO
  * @author fanyanyan
  * @date 2018/12/4 15:14
  */
/**
  * DataFrame API基本操作
  */
object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    // 将json文件加载成一个dataframe
    val peopleDF = spark.read.format("json").load("E:\\IDEA\\IdeaProject\\SparkSQLProject\\data\\people.json")

    // 输出dataframe对应的schema信息
    peopleDF.printSchema()

    // 输出数据集的前20条记录
    peopleDF.show()

    // 查询某列所有的数据：select name from emp
    peopleDF.select("name").show()

    // 查询某几列所有的数据，并对列进行计算：select name,age+10 from table
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age2")).show()

    // 根据某一列的值进行过滤：select * from table where age > 19 (或者使用where)
    peopleDF.filter(peopleDF.col("age") > 19).show()

    // 根据某一列进行分组，然后再进行聚合操作:select age,count(1) from table group by age
    peopleDF.groupBy("age").count().show()


    spark.stop()

  }

}
