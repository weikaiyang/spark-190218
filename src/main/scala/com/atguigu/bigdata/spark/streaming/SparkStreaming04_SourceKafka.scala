package com.atguigu.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_SourceKafka {

    def main(args: Array[String]): Unit = {

        // 准备配置信息
        val sparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[2]")

        // 创建上下文环境对象
        val streamingContext = new StreamingContext(sparkConf, Seconds(3))

        // 从Kafka中获取数据
        val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
            streamingContext,
            Map(
                ConsumerConfig.GROUP_ID_CONFIG -> "atguiguGroup",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
                "zookeeper.connect" -> "linux1:2181"
            ),
            Map(
                "atguiguSparkStreaming" -> 3
            ),
            StorageLevel.MEMORY_ONLY
        )
        val lineDStream: DStream[String] = kafkaDStream.map {
            case (k, v) => v
        }

        // 将一行数据进行扁平化操作
        val wordDStream: DStream[String] = lineDStream.flatMap(line=>line.split(" "))

        // 将单词转换结构
        val wordToOneDStream: DStream[(String, Int)] = wordDStream.map(word=>(word,1))

        // 聚合数据
        val resultDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_+_)

        resultDStream.print()

        // 释放资源
        // SparkStreaming的采集器需要长期执行，所以不能停止
        // SparkStreaming的采集器需要明确启动
        streamingContext.start()
        //streamingContext.stop()

        // Driver程序不能单独停止，需要等待采集器的执行结束
        streamingContext.awaitTermination()

    }
}
