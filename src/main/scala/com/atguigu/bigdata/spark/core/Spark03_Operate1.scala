package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Operate1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        // 算子 - mapPartitionsWithIndex
        // 算子传递的参数：分区号，分区数集合
        /*
        val mapRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex {
            (index, datas) => {
                datas.map((_, index))
            }
        }
        mapRDD.collect().foreach(println)
        */

        // 算子 - flatMap - 扁平化
        // 将一个整体数据拆分分成一个一个的个体进行数据操作,而不是将整体数据进行操作
        // line => word
        //val listRDD = sc.makeRDD(List(List(1,2), List(3,4), List(5,6)))
        //val numRDD = sc.makeRDD(List(1,2,3,4))
        //val numRDD = sc.makeRDD(List(1,List(2,5),3,4))
        // list.map
        // int.map
        //val flatMapRDD: RDD[Int] = listRDD.flatMap(list=>list)
        //val flatMapRDD: RDD[Int] = listRDD.flatMap(list=>list)
        //val flatMapRDD: RDD[Int] = numRDD.flatMap(num=>List(num))

        // 1,2,3,4,5,6
        // 1,2,5,3,4
        //flatMapRDD.collect().foreach(println)

        // 算子 - glom - 分区分组
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
        //val glomRDD: RDD[Array[Int]] = rdd.glom()
        //glomRDD.collect().foreach(datas=>println(datas.max))
        println(rdd.collect().max)

        sc.stop()

    }
}
