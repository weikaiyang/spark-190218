package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL02_Demo1 {

    def main(args: Array[String]): Unit = {

        // 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

        // 准备环境对象
        //val spark = new SparkSession(sparkConf)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // 将RDD, DataFrame, DataSet进行转换时，需要隐式转换规则
        // 此时spark不是包名，是SparkSession对象名称
        import spark.implicits._

        // RDD , DataFrame, DataSet
        val dataRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan", 30), (2,"lisi", 40), (3,"wangwu", 20)))

        // 在RDD基础上增加结构信息，转换为DataFrame
        val df: DataFrame = dataRDD.toDF("id", "name", "age")

        // 在DataFrame的基础上增加类型信息，转换为DataSet
        val ds: Dataset[User] = df.as[User]

        // 将DataSet转化为DataFrame
        val df1: DataFrame = ds.toDF()

        // 将DataFrame转换为RDD
        val rdd1: RDD[Row] = df1.rdd

        rdd1.foreach(row=>{
            // 从Row对象中获取数据，传递的值为索引
            println(row.getInt(0) + "," + row.getString(1))
        })

        //ds.show()

        // 释放资源
        spark.stop()

    }
}
case class User( id:Long, name:String, age:Long )
