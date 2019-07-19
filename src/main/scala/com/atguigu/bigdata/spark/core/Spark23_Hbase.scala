package com.atguigu.bigdata.spark.core

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_Hbase {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val conf: Configuration = HBaseConfiguration.create()
        //conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
        conf.set(TableInputFormat.INPUT_TABLE, "test")


        // 查询Hbase数据库
        val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
            conf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result])


        // 循环数据
        hbaseRDD.foreach{
            case (rk, result) => {
                for (cell <- result.rawCells() ) {
                    println(Bytes.toString(CellUtil.cloneValue(cell)))
                }
            }
        }

        sc.stop()

    }
}
