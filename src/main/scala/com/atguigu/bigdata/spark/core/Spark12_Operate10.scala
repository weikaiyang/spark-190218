package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Operate10 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        // 算子 - aggregateByKey
        val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

        // aggregateByKey : 根据key来聚合数据
        // 需要传递两个参数列表
        // 第一个参数列表（零值）
        // 第二个参数列表（分区内计算逻辑，分区间计算逻辑）

        // 将RDD中的数据分区内取最大值，然后分区间相加
        //rdd.aggregateByKey()((x,y)=>x, (x,y)=>x+y)

        // ("a",3),("a",2),("c",4)
        //    ==> (a,10), (c,10)
        // ("b",3),("c",6),("c",8)
        //    ==> (b, 10), (c,10)

        // ==> (a,10)(b,10)(c,20)

        //val resultRDD: RDD[(String, Int)] = rdd.aggregateByKey(10)((x,y)=>{math.max(x,y)}, (a,b)=>{a+b})

        // ("a",3),("a",2),("c",4)
        //    ==> (a,5), (c,4)
        // ("b",3),("c",6),("c",8)
        //    ==> (b, 3), (c,14)
        //val resultRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)


        // 算子 - foldByKey
        //val resultRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
        //resultRDD.collect().foreach(println)

        // 算子 - combineByKey

        // 根据key计算每种key的均值
        // ("a", 88), ("b", 95), ("a", 91)
        // ("b", 93), ("a", 95), ("b", 98)

        // a => List(88, 91, 95) => list.sum / list.siz
        // a => List((88,1),(91,1),(95,1)) => (sum, count) => sum / count
        // b => List(95, 93, 98) => list.sum / list.size

        val input = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)

        // combineByKey需要传递三个参数



        // 第一个参数：转换数据的结构 88 => (88,1)
        // 第二个参数：分区内计算规则
        // 第三个参数：分区间计算规则
        val resultRDD: RDD[(String, (Int, Int))] = input.combineByKey(
            v => (v, 1),
            (t: (Int, Int), v) => {
                (t._1 + v, t._2 + 1)
            },
            (t: (Int, Int), t1: (Int, Int)) => {
                (t._1 + t1._1, t._2 + t1._2)
            }
        )
        resultRDD.map{
            case (k, v) => {
                (k, v._1/v._2)
            }
        }.collect().foreach(println)

        sc.stop()

    }
}