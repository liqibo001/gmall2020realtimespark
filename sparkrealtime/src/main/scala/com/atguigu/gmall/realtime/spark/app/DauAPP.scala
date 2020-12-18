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
    //创建SparkStreaming环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
    val ssc = new StreamingContext(conf, Seconds(5))
    val groupId = "dau_appgroup"
    val topic = "ODS_BASE_LOG" // operational data store
    //从kafka 接收数据
    val recordInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    //处理数据  0    1
    //    recordInputDstream.map(_.value()).print();

    //数据整理格式
    val jsonDstream: DStream[JSONObject] = recordInputDstream.map { record =>
      val jsonObject: JSONObject = JSON.parseObject(record.value())
      val ts: lang.Long = jsonObject.getLong("ts")
      val dateFormat = new SimpleDateFormat("yyyy-MMM-dd HH")
      val dateTimeStr: String = dateFormat.format(new Date(ts))
      val dateTimeArr: Array[String] = dateTimeStr.split(" ")
      jsonObject.put("dt", dateTimeArr(0))
      jsonObject.put("hr", dateTimeArr(1))
      jsonObject
    }

    val firstVisitDstream: DStream[JSONObject] = jsonDstream.filter(jsonObj => {
      val pageJson: JSONObject = jsonObj.getJSONObject("page")
      if (pageJson != null) {
        val lastPageId: String = pageJson.getString("last_page_id")
        if (lastPageId == null || lastPageId.length == 0) {
          true
        } else {
          false
        }
      } else {

        false
      }
    })
    firstVisitDstream.print(100)

    //去重 使用Redis进行去重
    firstVisitDstream.mapPartitions { jsonItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val listBuffer = new ListBuffer[JSONObject]()
      for (jsonObj <- jsonItr) {
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val dt: String = jsonObj.getString("dt")
        val dauKey = "dut" + dt
        val noExists: lang.Long = jedis.sadd(dauKey, mid) //0已存在  1 不存在
        jedis.expire(dauKey, 24 * 3600)
        jedis.close()
        if (noExists == 1L) {
          listBuffer.append(jsonObj)
        }
      }
      jedis.close()
      listBuffer.toIterator
    }

    //输出数据

    ssc.start()
    ssc.awaitTermination()

  }

}
