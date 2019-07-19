package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark20_JSON {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        import scala.util.parsing.json.JSON

        val json = sc.textFile("input/user.json")

        // Spark读取JSON文件时，要求文件内容的每一行符合JSON格式
        val result  = json.map(JSON.parseFull)

        result.collect().foreach(println)

        sc.stop()

    }
}
