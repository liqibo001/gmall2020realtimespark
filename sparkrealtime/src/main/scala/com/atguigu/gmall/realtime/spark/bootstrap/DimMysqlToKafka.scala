package com.atguigu.gmall.realtime.spark.bootstrap

import java.util

import com.alibaba.fastjson.JSONObject
import com.atguigu.gmall.realtime.spark.util.{MySqlUtil, MykafkaSink}

object DimMysqlToKafka {
  def main(args: Array[String]): Unit = {
    val dimTables = Array("user_info", "sku_info", "base_province")
    for (tableName <- dimTables) {
      val datalist: util.List[JSONObject] = MySqlUtil.queryList("select * from "+tableName)
      val canalJSONObj = new JSONObject()
      canalJSONObj.put("data",datalist)
      canalJSONObj.put("table",tableName)
      canalJSONObj.put("type","INSERT")
      canalJSONObj.put("pkNames",util.Arrays.asList("id"))

     println(canalJSONObj.toJSONString)

      MykafkaSink.send("ODS_BASE_DB_C",canalJSONObj.toString())
    }
  }
}
