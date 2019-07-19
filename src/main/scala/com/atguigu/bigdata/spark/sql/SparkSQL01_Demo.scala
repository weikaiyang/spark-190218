package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_Demo {

    def main(args: Array[String]): Unit = {

        // 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

        // 准备环境对象
        //val spark = new SparkSession(sparkConf)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // DataFrame的创建
        val df: DataFrame = spark.read.json("input/user.json")

        // 创建临时表
        //df.createOrReplaceTempView("user")
        // 通过SQL语法访问DF
        //val sqlDF: DataFrame = spark.sql("select * from user")
        // 访问数据
        //sqlDF.show()

        // 使用DSL语法访问数据
        df.select("name", "age").show

        // 释放资源
        spark.stop()

    }
}
