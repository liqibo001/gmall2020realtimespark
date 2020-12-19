package com.atguigu.gmall.realtime.spark.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.spark.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


object DauAPP {
  def main(args: Array[String]): Unit = {
    //创建SparkStreaming 环境
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(conf, Seconds(5))

    //从kafka接收数据
    val groupId = "dau_app_group"
    val topic = "ODS_BASE_LOG"
    val recordInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)


    // 处理数据 1 数据整理 格式化日期格式
    val jsonDstream: DStream[JSONObject] = recordInputDstream.map { record => {
      val jSObject: JSONObject = JSON.parseObject(record.value())
      //整理日期 提取出日期和小时
      val ts: lang.Long = jSObject.getLong("ts")
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateTimeStr: String = dateFormat.format(new Date(ts))
      val dateTimeArr: Array[String] = dateTimeStr.split(" ")
      jSObject.put("dt", dateTimeArr(0))
      jSObject.put("hr", dateTimeArr(1))
      jSObject
    }
    }

    val value: DStream[JSONObject] = jsonDstream.mapPartitions {
      jsonItr => {
        val list: List[JSONObject] = jsonItr.toList
        println("未过滤数据" + list.size)
        list.toIterator
      }
    }


    //筛选出 用户首次访问的页面
    val firstVisitDstream: DStream[JSONObject] = value.filter(jsonObj => {
      var ifFirst = false
      val pageJson: JSONObject = jsonObj.getJSONObject("page")
      if (pageJson != null) {
        val lastPageId: String = pageJson.getString("last_page_id")
        if (lastPageId == null || lastPageId.length == 0) {
          ifFirst = true
        }
      }
      ifFirst
    })
    firstVisitDstream.print(100)




    //去重 使用Redis去重
    val dauJsonObjDstream: DStream[JSONObject] = firstVisitDstream.mapPartitions {
      jsonItr => {
        val jedis: Jedis = RedisUtil.getJedisClient
        val sourceList: List[JSONObject] = jsonItr.toList
        val rsList = new ListBuffer[JSONObject]()
        println("筛选前" + sourceList.size)
        for (jsonObj <- sourceList) {
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          val dt: String = jsonObj.getString("dt")
          val dauKey = "dau" + dt
          val nonExists: lang.Long = jedis.sadd(dauKey, mid) //返回0 表示已经存在  返回1 表示未存在
          jedis.expire(dauKey, 24 * 3600)
          if (nonExists == 1L) {
            rsList.append(jsonObj)
          }
        }
        jedis.close()
        println("筛选后：" + rsList.size)
        rsList.toIterator
      }
    }
    dauJsonObjDstream.print(100)



    ssc.start()
    ssc.awaitTermination()
  }

}
