package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Operate5 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val rdd = sc.parallelize(List(2,1,3,4))

        // 算子 - sortBy - 排序
        // 0,1,1,0
        // 1,3,2,4
        val numRDD: RDD[Int] = rdd.sortBy(num=>num%2, false)

        numRDD.collect().foreach(println)

        sc.stop()

    }
}
