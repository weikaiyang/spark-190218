package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

        // 创建Spark上下文环境对象
        // 默认的部署模式为local
        val sc = new SparkContext(sparkConf)

        // 创建RDD, 间接调用parallelize方法。推荐使用
        //val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        // 创建RDD
        //val rdd2: RDD[Int] = sc.parallelize(List(1,2,3,4))

        val rdd: RDD[Int] = sc.makeRDD(1 to 10)

        // Driver Code
        List(1,2,3,4)

        rdd.map(_*2)

        rdd.mapPartitions{
            datas=> {
                // Executor Code
                datas.map(_*2)
            }
        }

    }
}
