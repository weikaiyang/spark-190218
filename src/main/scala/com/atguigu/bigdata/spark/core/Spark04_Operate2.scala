package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Operate2 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        // 算子 - groupBy
        /*
        val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(num=>num)
        //val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(num=>num%2)

        groupRDD.collect().foreach(println)
        */

        // 算子 - filter
        val filterRDD: RDD[Int] = rdd.filter(num=>num%2==0)

        filterRDD.collect().foreach(println)

        sc.stop()

    }
}
