package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Operate3 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        //val rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

        // 算子 - sample - 采样

        // 三个参数：
        // 第一个参数：抽取数据后是否放回
        // 第二个参数：给数据打分
        // 第三个参数：随机数种子，用于随机打分
        //val sampleRDD: RDD[Int] = rdd.sample(false, 0.5, 4)
        //val sampleRDD: RDD[Int] = rdd.sample(true, 3)

        //sampleRDD.collect().foreach(println)

        // 算子 - distinct - 去重
        val rdd: RDD[Int] = sc.makeRDD(List(1,1,2,2,3,4,3,4), 2)

        //val distinctRDD: RDD[Int] = rdd.distinct()
        val distinctRDD: RDD[Int] = rdd.distinct(4)

        distinctRDD.collect().foreach(println)

        sc.stop()

    }
}
