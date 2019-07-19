package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Partitions {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        // 创建RDD
        // makeRDD函数会默认将当前CPU核数作为分区数量
        // 分区数量可以修改
        //val rdd: RDD[Int] = sc.makeRDD( List(1,2,3,4), 3 )

        // textFile函数默认最小分区为CPU核数和2取小值
        // 文件分区是以文件单位分区,每一个文件读取时以行的方式读取，所以并不是按照字节数读取.
        val rdd: RDD[String] = sc.textFile("input/1.txt", 2)

        rdd.saveAsTextFile("output")
        //println(rdd.partitions.length)

        sc.stop()

    }
}
