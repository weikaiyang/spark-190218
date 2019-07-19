package com.atguigu.bigdata.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark25_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

        //val i: Int = rdd.reduce(_+_)

        /*
        var sum = 0

        // 分布式计算
        rdd.foreach(num=>{
            sum = sum + num
        })

        println("sum = " + sum)
        */

        // 可以通知Spark，数据需要返回Drvier
        // 使用累加器可以实现此功能

        // 声明累加器
        // 注册累加器
        val sum: LongAccumulator = sc.longAccumulator("sum")

        rdd.foreach(num=>{
            // 使用累加器
            sum.add(num)
        })

        // 访问累加器
        println(sum.value)

        sc.stop()

    }
}
