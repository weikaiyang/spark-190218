package spark.test


import kafka.serializer._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkWordCount {
  def main(args: Array[String]): Unit = {
  val sparkConf: SparkConf = new SparkConf().setAppName("aaaaa").setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf,Seconds(3))
    val wordsStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](streamingContext,
      Map(ConsumerConfig.GROUP_ID_CONFIG -> "atguiguGroup",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        "zookeeper.connect" -> "hadoop102:2181"),
      Map("atguiguSparkStreaming" -> 3),
      StorageLevel.DISK_ONLY)

    val value: DStream[String] = wordsStream.map {
      case (k, v) => v
    }


    val words: DStream[String] =  value.flatMap(_.split(" "))
    val mapWords: DStream[(String, Int)] = words.map((_,1))
    val result: DStream[(String, Int)] = mapWords.reduceByKey(_+_)
    result.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

