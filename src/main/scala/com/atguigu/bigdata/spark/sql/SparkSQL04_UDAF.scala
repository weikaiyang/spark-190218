package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object SparkSQL04_UDAF {

    def main(args: Array[String]): Unit = {

        // 配置对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

        // 准备环境对象
        //val spark = new SparkSession(sparkConf)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // 将RDD, DataFrame, DataSet进行转换时，需要隐式转换规则
        // 此时spark不是包名，是SparkSession对象名称
        import spark.implicits._

        // 使用自定义聚合函数访问数据
        // 创建自定义聚合函数
        val udaf = new MyAgeAvgUDAF
        // 向spark注册
        spark.udf.register("ageAvg", udaf)

        // 使用聚合函数
        val dataRDD: RDD[(Long, String, Long)] = spark.sparkContext.makeRDD(List((1L, "xxxx", 20L),(2L, "yyy", 30L),(3L, "zzzz", 40L)))

        val df: DataFrame = dataRDD.toDF("id", "name", "age")

        df.createOrReplaceTempView("user")

        spark.sql("select ageAvg(age) from user").show

        // 释放资源
        spark.stop()

    }
}
// 声明自定义聚合函数：取年龄的平均值
class MyAgeAvgUDAF extends UserDefinedAggregateFunction {


    // 输入数据的结构
    override def inputSchema: StructType = {
        new StructType().add("age", LongType)
    }

    // 缓冲区数据的结构
    override def bufferSchema: StructType = {
        new StructType().add("sum", LongType).add("cnt", LongType)
    }

    // 聚合函数的结果类型
    override def dataType: DataType = {
        DoubleType
    }

    // 当前函数是否稳定
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L
        buffer(1) = 0L
    }

    // 更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1L
    }

    // 合并缓冲区数据
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // 计算
    override def evaluate(buffer: Row): Any = {
        buffer.getLong(0).toDouble / buffer.getLong(1)
    }
}