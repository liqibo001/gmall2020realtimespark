package com.atguigu.gmall.realtime.spark.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
import org.elasticsearch.search.builder.SearchSourceBuilder

object MyEsUtil {
  private var factory: JestClientFactory = null;

  def getClient: JestClient = {
    //如果工厂没有初始化 就创建工厂  有工厂就得到对象
    if (factory == null) {
      build()
    }
    factory.getObject
  }

  def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(1000).build())
  }

  def main(args: Array[String]): Unit = {
    saveDat()
  }


  def saveDat(): Unit = {
    val jest: JestClient = getClient
    //创建一个保存动作
    val index: Index = new Index.Builder(Movie("999", "急先锋"))
      .index("movie_test0820_2020-12-23").`type`("_doc").build()
    jest.execute(index)
    jest.close()
  }


  def saveBulkData(sourceList: List[(Any, String)], indexName: String): Unit = {
    if (sourceList != null && sourceList.size > 0) {
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder
      for ((data, id) <- sourceList) {
        val index: Index = new Index.Builder(data).index(indexName).`type`("_doc").id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val result: BulkResult = jest.execute(bulk)
      jest.close()
    }
  }

  //查询
  def queryData(): Unit = {
    val jest: JestClient = getClient
    val query = "{\n  \"query\": {\n     \"bool\": {\n       \"must\": [\n         {\"match\": {\n           \"name\": \"红海\"\n         }}\n       ],\n       \"filter\": {\n         \"term\": {\n           \"actorList.name\": \"张涵予\"\n         }\n       }\n     }\n  },\n  \"highlight\": {\n    \"fields\": {\"name\": {}}\n  }\n  , \"size\": 20\n  ,\"from\": 0\n  , \"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"desc\"\n      }\n    }\n  ]\n}"
    val searchSourceBuilder = new SearchSourceBuilder()
  }


  case class Movie(id: String, movie_name: String)

}
