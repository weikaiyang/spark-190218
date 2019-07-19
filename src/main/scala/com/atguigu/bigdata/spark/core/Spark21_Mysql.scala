package com.atguigu.bigdata.spark.core

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_Mysql {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        // 数据库的连接配置
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://linux1:3306/rdd"
        val userName = "root"
        val passWd = "000000"


        // 读取Mysql的数据
        val jdbc = new JdbcRDD(
            sc,
            ()=>{
                Class.forName(driver)
                DriverManager.getConnection(url, userName, passWd)
            },
            "select * from user where id >= ? and id <= ?",
            1,
            3,
            3,
            (rs)=>{
                rs.getInt(1)
                println(rs.getString("name") + ":" + rs.getInt("age"))
            }
        )

        jdbc.collect()


        sc.stop()

    }
}
