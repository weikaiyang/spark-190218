package com.atguigu.bigdata.spark.core

import java.util
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object Spark26_Accumulator {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val wordRDD = sc.makeRDD(List("hello", "scala", "hbase", "spark"))

        // 创建累加器
        val acc = new MyAccumulator;
        // 注册累加器
        sc.register(acc, "blackNameList")

        wordRDD.foreach(word=>{
            // 操作累加器
            acc.add(word)
        })

        // 访问累加器
        println(acc.value)

        sc.stop()

    }
}
// 自定义累加器
// 将不合法的单词抽取出来，形成黑名单
// 1) 继承类
// 2) 重写方法
// 3) 创建累加器
class MyAccumulator extends AccumulatorV2[String, util.HashSet[String]] {

    // 黑名单集合
    val blackNameSet = new util.HashSet[String]()

    // 是否为初始化
    override def isZero: Boolean = {
        blackNameSet.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, util.HashSet[String]] = {
        new MyAccumulator
    }

    // 重置累加器
    override def reset(): Unit = {
        blackNameSet.clear()
    }

    // 增加数据
    override def add(word: String): Unit = {
        if ( word.contains("h") ) {
            blackNameSet.add(word)
        }
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[String, util.HashSet[String]]): Unit = {
        blackNameSet.addAll(other.value)
    }

    // 获取累加器的值
    override def value: util.HashSet[String] = {
        blackNameSet
    }
}