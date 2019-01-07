package com.yifeng.restclient.utils.request_sender;

/**
 * Created by guoyifeng on 12/20/18
 */

import com.alibaba.fastjson.JSONObject;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

/**
 * This class aims to de-couple the current 2.x and 5.x elasticsearch requests including search and aggregation <br>
 * by offering several methods which hide the compatibility details and differences when developing with different <br>
 * es versions
 */
public interface ElasticsearchRequestSender {

    ElasticsearchRequestSender index(String index);

    ElasticsearchRequestSender type(String type);

    ElasticsearchRequestSender query(QueryBuilder qb);

    ElasticsearchRequestSender aggregation(NewAggregationRequestGenerator generator);

    JSONObject performRequest(String method, String endpoint, Map<String, String> params, HttpEntity entity, Header... headers);

    String getRequest();

    HttpEntity getRequestEntity();
}
