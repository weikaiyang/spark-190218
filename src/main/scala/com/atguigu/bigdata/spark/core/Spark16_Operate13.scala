package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_Operate13 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",2), ("a", 3), ("a", 4)))

        val stringToLong: collection.Map[String, Long] = dataRDD.countByKey()

        println(stringToLong)

        //val strRDD: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Scala"))

        //val stringToLong: collection.Map[String, Long] = strRDD.countByValue()
        //println(stringToLong)

        sc.stop()

    }
}