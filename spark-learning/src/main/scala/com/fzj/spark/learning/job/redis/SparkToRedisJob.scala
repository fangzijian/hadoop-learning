package com.fzj.spark.learning.job.redis

import com.fzj.spark.learning.cases.Domain
import com.fzj.spark.learning.util.{RedisUtil, SparkUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}
import redis.clients.pipeline.AsyncCommand

import scala.collection.mutable.ListBuffer

class SparkToRedisJob(spark: SparkSession) extends Serializable {
  def work(): Any = {
    val frame = loadHiveDate()
    dfIntoRedis(frame)
  }

  def loadHiveDate(): DataFrame = {
    val sqlStr =
      s"""
         |select *
         |form tableA
         |where dayno = 20210603
         |""".stripMargin
    val df = spark.sql(sqlStr).cache()
    df
  }

  def dfIntoRedis(dataFrame: DataFrame): Int = {
    import spark.implicits._
    val partitionCount = 100000
    val importResult = dataFrame.repartition(partitionCount).mapPartitions(
      partition => {
        val resultList = new ListBuffer[(Int, Int)]()
        val pipeline = RedisUtil.getPipeline("127.0.0.1")
        partition.foreach(row => {
          val redisKey = "fzj"
          val value = row.getString(1)

          pipeline.set(new AsyncCommand {
            override def onSuccess(): Unit = {
            }

            override def onFailure(): Unit = {
            }
          }, redisKey, value)
        })
        pipeline.close()
        //返回一个list
        resultList.iterator
      }).collect()
    importResult.map(_._1).sum
  }
}

object SparkToRedisJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.initSparkSession(
      Array(classOf[ImmutableRoaringBitmap],
        classOf[MutableRoaringBitmap],
        classOf[Domain]),
      enableCrossJoin = true)
    new SparkToRedisJob(spark).work()
  }
}
