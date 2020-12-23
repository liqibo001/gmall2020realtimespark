package com.atguigu.publisher.service.impl;

import com.atguigu.publisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class DauServiceImpl implements DauService {

    @Autowired    //自动注入 装配
            JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String indexName = "gmall2020_dau_info" + date + "-query";
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        Long total;
        try {
            SearchResult result = jestClient.execute(search);
            total= result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw  new RuntimeException("es 查询异常");
        }
        return total;
    }

    @Override
    public Map getDauHourCount(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggsBuilder = AggregationBuilders.terms("groupby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggsBuilder);
        String indexName = "gmall2020_dau_info" + date + "-query";

        //构造查询动作
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        Map rsMap=new HashMap();

        try {
            SearchResult searchResult = jestClient.execute(search);
            TermsAggregation groupby_hr = searchResult.getAggregations().getTermsAggregation("groupby_hr");
            if (groupby_hr!=null){
                List<TermsAggregation.Entry> buckets = groupby_hr.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    rsMap.put(bucket.getKey(),bucket.getCount());
                }
            }
            return rsMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }

    }
}
