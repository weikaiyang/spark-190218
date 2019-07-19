package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Transform {

    def main(args: Array[String]): Unit = {

        // 准备配置信息
        val sparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[2]")

        // 创建上下文环境对象
        val streamingContext = new StreamingContext(sparkConf, Seconds(3))

        // 使用窗口函数对多个采集周期的数据进行统计
        val lineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("linux1", 9999)
        // 将采集数据放置在窗口中
        val windowDStream: DStream[String] = lineDStream.window(Seconds(9),Seconds(6))

        // 将一行数据进行扁平化操作
        val wordDStream: DStream[String] = windowDStream.flatMap(line=>line.split(" "))

        // 将单词转换结构
        val wordToOneDStream: DStream[(String, Int)] = wordDStream.map(word=>(word,1))

        // 聚合数据
        val resultDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_+_)

        // 转换
        // Driver Coding (1)
        resultDStream.transform(rdd => {
            // transform的内部可以进行任意的RDD转换算子操作
            // Driver Coding (M)
            rdd.map(t=>{
                // Executor Coding(N)
                t
            })
        })
        // ****************************************
        // Drvier Coding (1)
        resultDStream.map(t=>{
            // Executor Coding (N)
            t
        })

        resultDStream.foreachRDD(rdd=>rdd.foreach(println))

        streamingContext.start()
        //streamingContext.stop()

        // Driver程序不能单独停止，需要等待采集器的执行结束
        streamingContext.awaitTermination()


    }
}
