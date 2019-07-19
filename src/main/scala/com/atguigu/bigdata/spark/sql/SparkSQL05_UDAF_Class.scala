package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{Encoder, _}


object SparkSQL05_UDAF_Class {

    def main(args: Array[String]): Unit = {

        // 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

        // 准备环境对象
        //val spark = new SparkSession(sparkConf)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // 将RDD, DataFrame, DataSet进行转换时，需要隐式转换规则
        // 此时spark不是包名，是SparkSession对象名称
        import spark.implicits._

        // 创建聚合函数
        val udaf = new MyAgeAvgClassUDAF
        // 向Spark注册
        //spark.udf.register("test", udaf)
        // 将聚合函数转换为查询列
        val col: TypedColumn[User1, Double] = udaf.toColumn.name("avgAge")

        val df: DataFrame = spark.read.json("input/user.json")

        // 使用DSL语法访问强类型聚合函数
        val ds: Dataset[User1] = df.as[User1]

        ds.select(col).show()

        // 释放资源
        spark.stop()

    }
}

case class User1( name : String, age : Long )
case class AvgBuff( var total:Long, var count:Long )

// 声明自定义聚合函数（强类型）：取年龄的平均值
class MyAgeAvgClassUDAF extends Aggregator[User1, AvgBuff, Double] {

    // 初始化
    override def zero: AvgBuff = {
        AvgBuff(0L, 0L)
    }

    // 聚合
    override def reduce(buff: AvgBuff, input: User1): AvgBuff = {

        buff.total = buff.total + input.age
        buff.count = buff.count + 1L

        buff
    }

    // 合并
    override def merge(buff1: AvgBuff, buff2: AvgBuff): AvgBuff = {

        buff1.total = buff1.total + buff2.total
        buff1.count = buff1.count + buff2.count

        buff1
    }

    // 完成计算
    override def finish(buff: AvgBuff): Double = {
        buff.total.toDouble / buff.count
    }

    override def bufferEncoder: Encoder[AvgBuff] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}