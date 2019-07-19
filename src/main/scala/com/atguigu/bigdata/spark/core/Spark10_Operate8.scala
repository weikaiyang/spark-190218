package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark10_Operate8 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        // K-V类型RDD
        val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        // 隐式转换 ： 类型转换
        // RDD当类型为K-V类型时，可以采用隐式转换自动变成PairRDDFunctions类型，然后调用其中的方法
        //numRDD.map( (_,1) ).reduceByKey(_+_)


        // 算子 - partitionBy
        val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

        // 重分区 ： 使用分区器来重新将数据进行分区
        // Partitioner : 分区器，用于将数据放置在指定的分区中, 使用数据的key进行分区
        //val rdd1 = sc.makeRDD(List(1,2,3,4))

        var rdd2 = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))
        //var rdd2 = rdd.partitionBy(new MyPartitioner(3))

        val mapRDD: RDD[(Int, (Int, String))] = rdd2.mapPartitionsWithIndex {
            (index, datas) => {
                datas.map((index, _))
            }
        }
        mapRDD.collect().foreach(println)

        sc.stop()

    }
}
// 自定义分区器
// 1. 继承Partitioner父类
class MyPartitioner(partitions: Int) extends Partitioner {
    // 获取分区数量
    override def numPartitions: Int = partitions

    // 通过数据的key返回所在的分区索引
    override def getPartition(key: Any): Int = {
        1
    }
}