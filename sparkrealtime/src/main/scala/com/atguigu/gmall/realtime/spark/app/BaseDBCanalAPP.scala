package com.atguigu.gmall.realtime.spark.app

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.spark.util.{MyKafkaUtil, MykafkaSink, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

object BaseDBCanalAPP {
  def main(args: Array[String]): Unit = {
    //创建SparkStreaming 环境
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(conf, Seconds(5))


    //从kafka接收数据
    val groupId = "DB_app_group"
    val topic = "ODS_BASE_DB_C"

    //从redis中得到偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    //判断offset是否有值 如果无值 取kafka默认偏移量
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]


    // 第一个分区键  第二个value
    val recordInputWithOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform {
      rdd =>
        //driver
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println(offsetRanges(0).untilOffset + "*****")
        rdd
    }

    val jsonObjDstream: DStream[JSONObject] = recordInputWithOffsetDstream.map(record => {
      JSON.parseObject(record.value())
    })
    jsonObjDstream.cache()
//    jsonObjDstream.print()

    val dimTables = Array("user_info", "sku_info", "base_province")

    jsonObjDstream.foreachRDD {
      rdd => {
        rdd.foreachPartition { jsonObjItr => {
          val jedis: Jedis = RedisUtil.getJedisClient
          for (jsonObj <- jsonObjItr) {
            //每条数据根据 table 和 type  放入不同的kafka topic中
            val table: String = jsonObj.getString("table")
            if (dimTables.contains(table)) {
              //维度表处理方式
              // type?  string   key? pk  value?  json  api? set
              val pks: JSONArray = jsonObj.getJSONArray("pkNames")
              if (pks!=null){
                val pkFieldName: String = pks.getString(0)
                val dataArr: JSONArray = jsonObj.getJSONArray("data")
                import scala.collection.JavaConverters._
                for (data <- dataArr.asScala) {
                  val dataJsonObj: JSONObject = data.asInstanceOf[JSONObject]
                  val pkValue: String = dataJsonObj.getString(pkFieldName)
                  val key = "DIM:" + table.toUpperCase() + ":" + pkValue
                  val value = dataJsonObj.toJSONString
                  jedis.set(key, value)
                }
              }
            } else {
              //事实表处理方式
              val optType: String = jsonObj.getString("type")
              var opt: String = ""
              if (optType == "INSERT") {
                opt = "I"
              } else if (optType == "UPDATE") {
                opt = "U"
              } else if (optType == "DELETE") {
                opt = "D"
              }

              if (opt.length > 0) {
                val topic = "DWD_" + table.toUpperCase() + "_" + opt
                val dataArr: JSONArray = jsonObj.getJSONArray("data")
                import scala.collection.JavaConverters._
                for (data <- dataArr.asScala) {
                  MykafkaSink.send(topic, data.toString)
                }
              }
            }

          }
          jedis.close()
        }
        }
      }
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
    }



    ssc.start()
    ssc.awaitTermination()
  }
}
