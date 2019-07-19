package com.atguigu.bigdata.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_Hbase1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val conf: Configuration = HBaseConfiguration.create()

        // 向Hbase写入数据
        // Put
        val dataRDD: RDD[(String, String)] = sc.makeRDD(List(("2001","XXXX"), ("2002", "YYYY"), ("2003", "ZZZZ")))

        val jobConf = new JobConf(conf)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test")

        // 将原始的数据结构进行转换
        val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
            case (rk, name) => {

                val rowkey = Bytes.toBytes(rk)
                val put = new Put(rowkey)
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))

                (new ImmutableBytesWritable(rowkey), put)
            }
        }

        putRDD.saveAsHadoopDataset(jobConf)

        sc.stop()

    }
}
