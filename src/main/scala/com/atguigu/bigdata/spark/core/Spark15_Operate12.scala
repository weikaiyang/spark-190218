package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_Operate12 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        numRDD.saveAsTextFile("output")
        //numRDD.saveAsObjectFile("output1")
        numRDD.map((_,1)).saveAsSequenceFile("output2")

        sc.stop()

    }
}