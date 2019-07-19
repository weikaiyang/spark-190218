package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark11_Operate9 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)




        // 算子 - groupByKey
        // (Int, String)
        /*
        val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(1,"ccc"),(2,"ddd")))

        // (Int, Iterable[String])
        // (1, List("aaa", "ccc")), (2, List("bbb", "ddd"))
        val groupRDD: RDD[(Int, Iterable[String])] = rdd.groupByKey()

        groupRDD.collect().foreach(println)
        */

        // 算子 - reduceByKey
        val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)))

        val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)

        reduceRDD.collect.foreach(println)

        sc.stop()

    }
}