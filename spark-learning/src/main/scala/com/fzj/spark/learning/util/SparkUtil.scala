package com.fzj.spark.learning.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Author rulo
 * @Date 2021/3/23 19:28
 */
object SparkUtil {

  def initSparkSession(registerKryoClasses: Array[Class[_]], enableCrossJoin: Boolean = false): SparkSession = {

    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(registerKryoClasses)
    if (enableCrossJoin) {
      sparkConf.set("spark.sql.crossJoin.enabled", "true")
    }
    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.addResource("core-site.xml")
    spark.sparkContext.hadoopConfiguration.addResource("hdfs-site.xml")

    spark
  }

}
