package com.yifeng.restclient.utils.request_sender;

/**
 * Created by guoyifeng on 12/12/18
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewAggregationRequestGenerator {

    private static Logger LOG = LoggerFactory.getLogger(NewAggregationRequestGenerator.class);

    private JSONObject queryBuilder;

    public NewAggregationRequestGenerator() {
        this.queryBuilder = new JSONObject();
    }

    /**
     * configure the maximum amount of hits to be returned in hits[]
     * @return
     */
    public NewAggregationRequestGenerator size(int size) {
        this.queryBuilder.put("size", size);
        return this;
    }

    public NewAggregationRequestGenerator from(int from) {
        this.queryBuilder.put("from", from);
        return this;
    }

    public NewAggregationRequestGenerator aggregation(CustomAggregationBuilder builder) {
        this.queryBuilder.put("aggregations", builder.getAggregationBuilder());
        return this;
    }

    /**
     *
     * @param qb This JSONObject works as QueryBuilder but has been processed by {@link ElasticsearchFieldAdaptor} if needed
     * @return
     */
    public NewAggregationRequestGenerator wrapQuery(JSONObject qb) {
        this.queryBuilder.put("query", JSONObject.parseObject(qb.toString()));
        return this;
    }

    public NewAggregationRequestGenerator query(QueryBuilder qb) {
        this.queryBuilder.put("query", JSONObject.parseObject(qb.toString()));
        return this;
    }

    public NewAggregationRequestGenerator queryString(String dsl) {
        JSONObject stringQuery = new JSONObject();
        JSONObject query = new JSONObject();
        query.put("query", dsl);
        stringQuery.put("query_string", query);
        this.queryBuilder.put("query", stringQuery);
        return this;
    }

    public String getRequest() {
        return JSON.toJSONString(this.queryBuilder);
    }
}
