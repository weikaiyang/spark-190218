package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Operate7 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        // Can only zip RDDs with same number of elements in each partitionva
        // 两个RDD中每个分区的元素的数量相同才能进行拉链处理
        // Can't zip RDDs with unequal numbers of partitions: List(2, 3)
        // RDD的拉链效果必须保证两个RDD具有相同的分区数
        val rdd1 = sc.parallelize(List(1,2,3,4),2)
        val rdd2 = sc.parallelize(List(4,5,6,7),3)

        // 拉链
        val zipRDD: RDD[(Int, Int)] = rdd1.zip(rdd2)

        zipRDD.collect().foreach(println)

        sc.stop()

    }
}
