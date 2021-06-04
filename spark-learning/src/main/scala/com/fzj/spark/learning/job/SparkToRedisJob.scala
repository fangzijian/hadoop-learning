package com.fzj.spark.learning.job

import com.fzj.spark.learning.`case`.DOMAIN
import com.fzj.spark.learning.util.{RedisUtil, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}
import redis.clients.pipeline.AsyncCommand

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer

class SparkToRedisJob(spark: SparkSession) extends Serializable {
  def work(): Any = {
    val frame = loadHiveTable()
    dataIntoRedis(frame)
  }

  def loadHiveTable(): DataFrame = {
    val sqlStr =
      s"""
         |select *
         |where dayno=20210603
         |""".stripMargin
    val invertDictDf = spark.sql(sqlStr).cache()
    invertDictDf
  }

  def dataIntoRedis(dataFrame: DataFrame): Int = {
    import spark.implicits._
    val partitionCount = 100000
    val importResult = dataFrame.repartition(partitionCount).mapPartitions(
      partition => {
        val resultList = new ListBuffer[(Int, Int)]()
        val successAccumulator = new AtomicInteger(0)
        val pipeline = RedisUtil.getPipeline("127.0.0.1")
        partition.foreach(row => if (StringUtils.isNotBlank(row.getString(0)) && StringUtils.isNotBlank(row.getString(1))) {
          val redisKey = "fzj"
          val field = row.getString(0)
          val value = row.getString(1)

          pipeline.hset(new AsyncCommand {
            override def onSuccess(): Unit = {
              successAccumulator.incrementAndGet()
            }

            override def onFailure(): Unit = {
            }
          }, redisKey, field, value)
        })
        pipeline.close()
        resultList.append((successAccumulator.get(), successAccumulator.get()))
        resultList.iterator
      }).collect()
    val successCount = importResult.map(_._1).sum
    successCount
  }
}

object SparkToRedisJob {

  def main(args: Array[String]): Unit = {
    // 需要开启允许笛卡儿积计算
    val spark = SparkUtil.initSparkSession(Array(classOf[ImmutableRoaringBitmap], classOf[MutableRoaringBitmap], classOf[DOMAIN]), enableCrossJoin = true)
    new SparkToRedisJob(spark).work()
  }
}
