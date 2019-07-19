package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_Operate15 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

        // 对RDD逻辑进行封装时，为了防止序列化异常，所以封装类都要是序列化接口
        val s = new Search("h")

        //val filterRDD: RDD[String] = s.getMatch1(rdd)
        val filterRDD: RDD[String] = s.getMatch2(rdd)

        filterRDD.foreach(println)

        sc.stop()

    }
}
//class Search(query:String) extends Serializable {
class Search(query:String)  {

    //过滤出包含字符串的数据
    def isMatch(s: String): Boolean = {
        s.contains(query)
    }

    //过滤出包含字符串的RDD
    def getMatch1 (rdd: RDD[String]): RDD[String] = {
        rdd.filter(isMatch)
    }

    //过滤出包含字符串的RDD
    def getMatch2(rdd: RDD[String]): RDD[String] = {
        val q : String = query
        rdd.filter(x => x.contains(q))
    }

}
