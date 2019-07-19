package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Operate4 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)


        // 改变分区
        val rdd = sc.parallelize(1 to 16,4)

        // 算子 - coalesce - 改变分区:不使用shuffle
        // 合并分区 : 当前使用的场合不会进行shuffle操作。
        // 在某些情况下，为了防止数据倾斜，必须要进行shuffle

        // coalesce算子有3个参数，其中第二个参数决定改变分区时，是否进行shuffle操作，默认不shuffle
        /*
        val coalesceRDD = rdd.coalesce(3)

        val mapRDD: RDD[(Int, Int)] = coalesceRDD.mapPartitionsWithIndex {
            (index, datas) => {
                datas.map((index, _))
            }
        }
        mapRDD.collect().foreach(println)
        */

        // 算子 - repartition - 改变分区:使用shuffle
        // 从底层代码中，可以看出，repartition其实就是coalesce，只不过，必须使用shuffle而已。
        val repartitionRDD: RDD[Int] = rdd.repartition(3)
        val mapRDD: RDD[(Int, Int)] = repartitionRDD.mapPartitionsWithIndex {
            (index, datas) => {
                datas.map((index, _))
            }
        }
        mapRDD.collect().foreach(println)

        sc.stop()

    }
}
