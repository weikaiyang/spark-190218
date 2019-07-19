package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Operate6 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.parallelize(List(1,2,3,4))
        val rdd2 = sc.parallelize(List(4,5,6,7))

        // 算子 - union - 合并
        val unionRDD: RDD[Int] = rdd1.union(rdd2)

        // 算子 - subtract  - 差集
        val subtractRDD: RDD[Int] = rdd1.subtract(rdd2)

        // 算子 - intersection  - 交集
        val intersectionRDD: RDD[Int] = rdd1.intersection(rdd2)

        // 算子 - cartesian  - 笛卡尔积
        val cartesianRDD: RDD[(Int, Int)] = rdd1.cartesian(rdd2)

        cartesianRDD.collect().foreach(println)

        sc.stop()

    }
}
