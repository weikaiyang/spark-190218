package com.atguigu.bigdata.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark27_Test1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        // 1000万
        val listRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        // 1000万
        //val listRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))

        // shuffle
        // (a, (1,4)), (b, (2,5)), (c, (3,6))
        //val joinRDD: RDD[(String, (Int, Int))] = listRDD1.join(listRDD2)

        val list = List(("a", 4), ("b", 5), ("c", 6))

        // 没有使用shuffle,也可以完成业务功能
        // 当前的使用方式不推荐使用，可能造成内存溢出，可以采用广播变量实现
        // 声明广播变量
        val listBC: Broadcast[List[(String, Int)]] = sc.broadcast(list)


        val resultRDD: RDD[(Any, Any)] = listRDD1.map {
            case (k, v) => {
                var kk: Any = k
                var vv: Any = null
                // 使用广播变量
                for ((k1, v1) <- listBC.value) {
                    if (k == k1) {
                        vv = (v, v1)
                    }
                }

                (kk, vv)
            }
        }

        resultRDD.foreach(println)

        sc.stop()

    }
}
