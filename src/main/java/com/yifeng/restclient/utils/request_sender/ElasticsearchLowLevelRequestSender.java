package com.yifeng.restclient.utils.request_sender;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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

import static com.yifeng.restclient.utils.request_sender.ElasticsearchFieldAdaptor.*;
/**
 * Created by guoyifeng on 12/20/18
 */

/**
 * different from {@link ElasticsearchHighLevelRequestSender} some methods shall <br>
 * do some extra job to make it work well on es 2.x
 */
public class ElasticsearchLowLevelRequestSender implements ElasticsearchRequestSender {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchLowLevelRequestSender.class);

    private ElasticsearchConnection connection;

    private NewAggregationRequestGenerator generator;

    private String index;

    private String type;

    private QueryBuilder qb;

    private boolean hasQuery;

    public ElasticsearchLowLevelRequestSender(ElasticsearchConnection connection) {
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
        this.qb = qb;
        if (isValid(qb)) {
            generator.query(qb);
        } else {
            generator.wrapQuery(validateQuery(qb));
        }
        hasQuery = true;
        return this;
    }

    @Override
    public ElasticsearchRequestSender aggregation(NewAggregationRequestGenerator generator) {
        this.generator = generator;
        if (hasQuery) query(qb);  // avoid the case when aggregation() is invoked after query(), the former generator will be overridden
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

    private boolean isValid(QueryBuilder qb) {
        JSONObject obj = JSONObject.parseObject(qb.toString());

        // case 1 single split_on_whitespace in query_string
        if (obj.containsKey("query_string")) {
            if (obj.getJSONObject("query_string").containsKey("split_on_whitespace")) {
                return false;
            }
        }

        // case 2 in must or must_not array
        if (obj.containsKey("bool")) {
            if (obj.getJSONObject("bool").containsKey("must")) {
                JSONArray must = obj.getJSONObject("bool").getJSONArray("must");
                for (int i = 0; i < must.size(); i++) {
                    if (must.getJSONObject(i).containsKey("exists") || must.getJSONObject(i).containsKey("query_string")) return false;
                }
            }
            if (obj.getJSONObject("bool").containsKey("must_not")) {
                JSONArray mustNot = obj.getJSONObject("bool").getJSONArray("must_not");
                for (int i = 0; i < mustNot.size(); i++) {
                    if (mustNot.getJSONObject(i).containsKey("exists") || mustNot.getJSONObject(i).containsKey("query_string")) return false;
                }
            }
        }

        return true;
    }

    private JSONObject validateQuery(QueryBuilder qb) {
        JSONObject obj = JSONObject.parseObject(qb.toString());

        // case 1 single split_on_whitespace in query_string
        if (obj.containsKey("query_string")) {
            if (obj.getJSONObject("query_string").containsKey("split_on_whitespace")) {
                obj = removeWhiteSpaceFromQueryString(obj);
            }
        }

        // case 2 in must or must_not array
        if (obj.containsKey("bool")) {
            if (obj.getJSONObject("bool").containsKey("must")) {
                JSONArray must = obj.getJSONObject("bool").getJSONArray("must");
                for (int i = 0; i < must.size(); i++) {
                    if (must.getJSONObject(i).containsKey("exists")) {
                        obj =  removeBoostInMust(obj);
                    }
                    if (must.getJSONObject(i).containsKey("query_string")) {
                        obj =  removeWhiteSpaceInMust(obj);
                    }
                }
            }
            if (obj.getJSONObject("bool").containsKey("must_not")) {
                JSONArray mustNot = obj.getJSONObject("bool").getJSONArray("must_not");
                for (int i = 0; i < mustNot.size(); i++) {
                    if (mustNot.getJSONObject(i).containsKey("exists")) {
                        obj =  removeBoostInMustNot(obj);
                    }
                    if (mustNot.getJSONObject(i).containsKey("query_string")) {
                        obj =  removeWhiteSpaceInMustNot(obj);
                    }
                }
            }
        }
        LOG.error("validate failed, please check the structure of the query");
        return obj;
    }
}
