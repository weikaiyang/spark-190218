package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // 准备配置信息
        val sparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[*]")

        // 创建上下文环境对象
        val streamingContext = new StreamingContext(sparkConf, Seconds(3))

        // 从指定端口获取数据
        val lineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102", 9999)

        // 将一行数据进行扁平化操作
        val wordDStream: DStream[String] = lineDStream.flatMap(line=>line.split(" "))

        // 将单词转换结构
        val wordToOneDStream: DStream[(String, Int)] = wordDStream.map(word=>(word,1))

        // 聚合数据
        val resultDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_+_)

        //resultDStream.print()
        //resultDStream.foreachRDD(rdd=>{})

        // 释放资源
        // SparkStreaming的采集器需要长期执行，所以不能停止
        // SparkStreaming的采集器需要明确启动
        streamingContext.start()
        //streamingContext.stop()

        // Driver程序不能单独停止，需要等待采集器的执行结束
        streamingContext.awaitTermination()

    }
}
