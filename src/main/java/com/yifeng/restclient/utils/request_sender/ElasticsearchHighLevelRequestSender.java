package com.yifeng.restclient.utils.request_sender;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yifeng.restclient.config.ElasticsearchConnection;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by guoyifeng on 12/20/18
 */
public class ElasticsearchHighLevelRequestSender implements ElasticsearchRequestSender {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchHighLevelRequestSender.class);

    private ElasticsearchConnection connection;

    private NewAggregationRequestGenerator generator;

    private String index;

    private String type;

    private QueryBuilder qb;

    private boolean hasQuery;

    public ElasticsearchHighLevelRequestSender(ElasticsearchConnection connection) {
        generator = new NewAggregationRequestGenerator();
        this.connection = connection;
    }

    @Override
    public ElasticsearchRequestSender index(String index) {
        this.index = index;
        return this;
    }

    @Override
    public ElasticsearchRequestSender type(String type) {
        this.type = type;
        return this;
    }

    @Override
    public ElasticsearchRequestSender query(QueryBuilder qb) {
        generator.query(qb);
        hasQuery = true;
        return this;
    }

    @Override
    public ElasticsearchRequestSender aggregation(NewAggregationRequestGenerator generator) {
        this.generator = generator;
        if (hasQuery) generator.query(qb); // avoid the case when aggregation() is invoked after query(), the former generator will be overridden
        return this;
    }

    @Override
    public JSONObject performRequest(String method, String endpoint, Map<String, String> params, HttpEntity entity, Header... headers) {
        try {
            Response response = this.connection.getLowLevelClient().performRequest(method, endpoint, params, entity, headers);
            HttpEntity httpEntity = response.getEntity();
            JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
            return res;
        } catch (Exception e) {
            LOG.error("error in performing request from ElasticsearchLowLevelRequestSender, {}", e);
        }
        return null;
    }

    @Override
    public String getRequest() {
        return this.generator.getRequest();
    }

    @Override
    public HttpEntity getRequestEntity() {
        return new NStringEntity(this.generator.getRequest(), ContentType.APPLICATION_JSON);
    }
}
