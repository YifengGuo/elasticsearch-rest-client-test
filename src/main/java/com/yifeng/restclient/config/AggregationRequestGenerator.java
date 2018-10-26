package com.yifeng.restclient.config;

/**
 * Created by guoyifeng on 10/25/18
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * this class is to supply several common aggregation wrap methods for elasticsearch low level rest client
 */
public class AggregationRequestGenerator {

    /**
     * this method is for simple term aggregation
     * request format is like:
     * {
     *   "from": 0,  // the offset from the first result you want to fetch
     *   "size": 0,  // configure the maximum amount of hits to be returned
     *   "aggregations": {
     *     "user_name": {
     *       "terms": {
     *         "field": "user_name",
     *         "size": 1000
     *       }
     *     }
     *   }
     * }
     * @return json string as query for the aggregation
     * eg. "{\"from\":0,\"size\":0,\"_source\":{\"includes\":[\"group\",\"COUNT\"],\"excludes\":[]},
     *      \"fields\":\"group\",\"aggregations\":{\"group\":{\"terms\":{\"field\":\"group\",\"size\":200},
     *      \"aggregations\":{\"group_member\":{\"value_count\":{\"field\":\"_index\"}}}}}}";
     */
    public static String simpleTermAggregation(int from, int hitSize, String termName, String field, int size) {
        JSONObject agg = new JSONObject();
        agg.put("from", from);
        agg.put("size", hitSize);
        JSONObject aggregations = new JSONObject();
        JSONObject aggregationName = new JSONObject();
        JSONObject aggregationType = new JSONObject();
        aggregationType.put("field", field);
        aggregationType.put("size", size);
        aggregationName.put("terms", aggregationType);
        aggregations.put(termName, aggregationName);
        agg.put("aggregations", aggregations);

        return JSON.toJSONString(agg);
    }

    public static String termAggregationWithQuery(int from, int hitSize, String termName, String field, int size, String dsl) {
        JSONObject agg = new JSONObject();
        agg.put("from", from);
        agg.put("size", hitSize);
        JSONObject aggregations = new JSONObject();
        JSONObject aggregationName = new JSONObject();
        JSONObject aggregationType = new JSONObject();
        aggregationType.put("field", field);
        aggregationType.put("size", size);
        aggregationName.put("terms", aggregationType);
        aggregations.put(termName, aggregationName);
        agg.put("aggregations", aggregations);

        JSONObject dslObj = JSON.parseObject(dsl);
        agg.put("query", dslObj.get("query"));

        return JSON.toJSONString(agg);
    }
}
