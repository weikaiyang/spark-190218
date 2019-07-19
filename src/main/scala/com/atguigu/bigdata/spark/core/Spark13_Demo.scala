package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Demo {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        // TODO 统计出每一个省份广告被点击次数的TOP3

        // TODO 1. 从文件中获取广告点击数据
        val lineRDD: RDD[String] = sc.textFile("input/agent.log")

        // TODO 2. 将数据进行结构的转换，保留用于统计分析的数据
        // (省份-广告，1)，(省份-广告，1)，(省份-广告，1)，(省份-广告，1)
        val priAdvToOneRDD: RDD[(String, Int)] = lineRDD.map {
            line => {
                val datas: Array[String] = line.split(" ")
                // 时间戳，省份，城市，用户，广告
                (datas(1) + "_" + datas(4), 1)
            }
        }

        // TODO 3. 将转换结构后的数据进行分组聚合
        // (省份-广告，sum), (省份-广告，sum), (省份-广告，sum)
        val priAdvToSumRDD: RDD[(String, Int)] = priAdvToOneRDD.reduceByKey(_+_)

        // TODO 4. 将聚合后的结果进行结构的转换
        // (省份, (广告，sum))，(省份, (广告，sum))，(省份, (广告，sum))
        /*
        priAdvToSumRDD.map(t=>{
            val keys: Array[String] = t._1.split("_")
            ( keys(0), (keys(1), t._2) )
        })
        */

        val priToAdvSumRDD: RDD[(String, (String, Int))] = priAdvToSumRDD.map {
            case (key, sum) => {
                val keys: Array[String] = key.split("_")
                (keys(0), (keys(1), sum))
            }
        }

        // TODO 5. 根据省份进行分组
        // （省份，Iterator[ (广告，sum), (广告，sum),(广告，sum), (广告，sum) ]）
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = priToAdvSumRDD.groupByKey()

        // TODO 6. 对广告点击数据进行排序（降序）后，取前三名（Top3）
        val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
            datas.toList.sortWith {
                (left, right) => {
                    left._2 > right._2
                }
            }.take(3)
        })


        // TODO 7. 将结果打印在控制台
        resultRDD.collect().foreach(println)
        //val newRDD: RDD[(String, List[(String, Int)])] = sc.makeRDD(resultRDD.collect())

        //newRDD.collect()

        sc.stop()

    }
}