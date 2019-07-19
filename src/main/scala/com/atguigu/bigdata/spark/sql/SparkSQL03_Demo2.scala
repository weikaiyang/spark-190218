package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL03_Demo2 {

    def main(args: Array[String]): Unit = {

        // 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

        // 准备环境对象
        //val spark = new SparkSession(sparkConf)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // 将RDD, DataFrame, DataSet进行转换时，需要隐式转换规则
        // 此时spark不是包名，是SparkSession对象名称
        import spark.implicits._

        // RDD, DataSet
        val dataRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan", 30), (2,"lisi", 40), (3,"wangwu", 20)))

        // RDD ==> DataSet
        // 样例类同时拥有结构和类型，所以可以将RDD的数据转换为样例类对象后，再转换为DataSet
        val userRDD: RDD[User] = dataRDD.map {
            case (id, name, age) => {
                User(id, name, age)
            }
        }
        val userDS: Dataset[User] = userRDD.toDS()
        userDS.show()

        // DataSet ==> RDD
        val rdd: RDD[User] = userDS.rdd
        rdd.foreach(user=>{
            println(user.id + "," + user.name + "," + user.age)
        })


        // 释放资源
        spark.stop()

    }
}
