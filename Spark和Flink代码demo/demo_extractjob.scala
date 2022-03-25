package com.shtd.contest.etl

import java.text.SimpleDateFormat
import java.util.Calendar

import com.shtd.contest.etl.extractjob.{MYSQLDBURL, MYSQLDRIVER, MYSQLPASSWORD, MYSQLUSER}
import org.apache.spark.sql.SparkSession

object demo_extractjob {


  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder()
    if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
      sparkBuilder.master("local[*]")
    }
    val spark = sparkBuilder.appName("demo_extractjob")
      .enableHiveSupport()
      .getOrCreate()


    /**
      * 连接mysql
      **/
    spark.read.format("jdbc")
      .option("url", MYSQLDBURL)  //mysql url
      .option("driver", MYSQLDRIVER) //mysql driver
      .option("user", MYSQLUSER) //mysql user
      .option("password", MYSQLPASSWORD) //mysql password
      .option("dbtable", "table1").load().createTempView("mysql_table1")




    //插入hive表
    spark.sql(
      s"""
         |insert overwrite table ods.table2 partition (fenquziduan='2021')
         |select * from mysql_table1
      """.stripMargin)
  }
}
