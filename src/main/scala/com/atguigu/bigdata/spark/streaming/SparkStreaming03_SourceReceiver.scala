package com.atguigu.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming03_SourceReceiver {

    def main(args: Array[String]): Unit = {

        // 准备配置信息
        val sparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[2]")

        // 创建上下文环境对象
        val streamingContext = new StreamingContext(sparkConf, Seconds(3))

        // 自定义采集器采集数据
        val lineDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("hadoop102", 9999))

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
// 声明采集器
// 1) 继承Receiver
// 2) 重写方法 onStart， onStop
class MyReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_SER_2) {

    private var socket: Socket = _

    // 接收数据
    def receive(): Unit = {
        try {
            socket = new Socket(host, port)
        } catch {
            case e: ConnectException =>
                return
        }

        // 接收数据
        val reader = new BufferedReader( new InputStreamReader( socket.getInputStream, "UTF-8" ) )//.getInputStream)
        var line : String = ""
        while ( (line = reader.readLine()) != null ) {
            if ( line.equals("==END==") ) {
                return
            } else {
                // 将数据转换为DStream
                store(line)
            }

        }

    }

    override def onStart(): Unit = {
        new Thread("Socket Receiver") {
            override def run() { receive() }
        }.start()
    }

    override def onStop(): Unit = {
        if ( socket != null ) {
            socket.close()
            socket = null
        }
    }
}
