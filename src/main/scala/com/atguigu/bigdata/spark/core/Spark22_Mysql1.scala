package com.atguigu.bigdata.spark.core

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_Mysql1 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        // 数据库的连接配置
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://linux1:3306/rdd"
        val userName = "root"
        val passWd = "000000"


        // 向Mysql写入数据
        // 1000
        val dataRDD: RDD[(Int, String, Int)] = sc.makeRDD(List((1,"zhangsan",20), (2,"lisi",30), (3,"wangwu",40)))

        /*
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = "insert into user(id, name, age) values (?, ?, ?)"
        val pstat: PreparedStatement = connection.prepareStatement(sql)

        dataRDD.foreach{
            case (id, name, age) => {

                // 操作数据库
                pstat.setInt(1, id)
                pstat.setString(2, name)
                pstat.setInt(3, age)

                pstat.executeUpdate()

            }
        }

        pstat.close()
        connection.close()
        */

        // 所有的连接对象没有办法序列化
        // foreachPartition类似于mapPartitions
        dataRDD.foreachPartition(datas=>{
            // Executor Coding
            Class.forName(driver)
            val connection: Connection = DriverManager.getConnection(url, userName, passWd)
            val sql = "insert into user(id, name, age) values (?, ?, ?)"
            val pstat: PreparedStatement = connection.prepareStatement(sql)

            datas.foreach{
                case (id, name, age) => {
                    // Executor
                    // 操作数据库
                    pstat.setInt(1, id)
                    pstat.setString(2, name)
                    pstat.setInt(3, age)

                    pstat.executeUpdate()

                }
            }

            pstat.close()
            connection.close()
        })


        sc.stop()

    }
}
