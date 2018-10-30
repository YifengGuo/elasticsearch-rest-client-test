package com.yifeng.restclient.config;

/**
 * Created by guoyifeng on 10/25/18
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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

    public static String metricsAggregationWithTimerange(int from, int hitSize, String aggName, String aggType, String field,
                                                         int size, String timestamp, long startTime, long endTime,
                                                         String flagField, boolean isExisted) {
        JSONObject agg = new JSONObject();
        agg.put("from", from);
        agg.put("size", hitSize);

        // aggregation
        JSONObject aggField = new JSONObject();
        JSONObject aggregations = new JSONObject();
        JSONObject aggregationType = new JSONObject();
        aggregationType.put("field", field);
        if (!aggType.equals("cardinality")) {
            aggregationType.put("size", size);
        }
        aggField.put(aggType, aggregationType);
        aggregations.put(aggName, aggField);
        agg.put("aggregations", aggregations);

        // query
        JSONObject query = new JSONObject();
        JSONObject outerBool = new JSONObject();
        JSONObject outerMust = new JSONObject();
        JSONObject innerBool = new JSONObject();
        JSONArray innerMust = new JSONArray();

        JSONObject obj1 = new JSONObject();
        JSONObject lowerRange = new JSONObject();
        JSONObject lowerRangeTimestamp = new JSONObject();
        lowerRangeTimestamp.put("from", startTime);
        lowerRangeTimestamp.put("to", null);
        lowerRangeTimestamp.put("include_lower", true);
        lowerRangeTimestamp.put("include_upper", true);
        lowerRange.put(timestamp, lowerRangeTimestamp);
        obj1.put("range", lowerRange);
        innerMust.add(obj1);

        JSONObject obj2 = new JSONObject();
        JSONObject upperRange = new JSONObject();
        JSONObject upperRangeTimestamp = new JSONObject();
        upperRangeTimestamp.put("from", null);
        upperRangeTimestamp.put("to", endTime);
        upperRangeTimestamp.put("include_lower", true);
        upperRangeTimestamp.put("include_upper", true);
        upperRange.put(timestamp, upperRangeTimestamp);
        obj2.put("range", upperRange);
        innerMust.add(obj2);

        // exist query
        // add this query based on boolean value
        if (isExisted) {
            JSONObject obj3 = new JSONObject();
            JSONObject mustNot = new JSONObject();
            JSONObject missing = new JSONObject();
            missing.put("field", flagField);
            mustNot.put("missing", missing);
            obj3.put("bool", mustNot);
            innerMust.add(obj3);
        } else {
            JSONObject obj3 = new JSONObject();
            JSONObject missing = new JSONObject();
            missing.put("field", flagField);
            obj3.put("missing", missing);
            innerMust.add(obj3);
        }


        innerBool.put("must", innerMust);
        outerMust.put("bool", innerBool);
        outerBool.put("must", outerMust);
        query.put("bool", outerBool);
        agg.put("query", query);

        return JSON.toJSONString(agg);
    }

    // overload method where this exists no bool query
    public static String metricsAggregationWithTimerange(int from, int hitSize, String aggName, String aggType, String field,
                                                         int size, String timestamp, long startTime, long endTime) {

        JSONObject agg = new JSONObject();
        agg.put("from", from);
        agg.put("size", hitSize);

        // aggregation
        JSONObject aggField = new JSONObject();
        JSONObject aggregations = new JSONObject();
        JSONObject aggregationType = new JSONObject();
        aggregationType.put("field", field);
        if (!aggType.equals("cardinality")) {
            aggregationType.put("size", size);
        }
        aggField.put(aggType, aggregationType);
        aggregations.put(aggName, aggField);
        agg.put("aggregations", aggregations);

        // query
        JSONObject query = new JSONObject();
        JSONObject outerBool = new JSONObject();
        JSONObject outerMust = new JSONObject();
        JSONObject innerBool = new JSONObject();
        JSONArray innerMust = new JSONArray();

        JSONObject obj1 = new JSONObject();
        JSONObject lowerRange = new JSONObject();
        JSONObject lowerRangeTimestamp = new JSONObject();
        lowerRangeTimestamp.put("from", startTime);
        lowerRangeTimestamp.put("to", null);
        lowerRangeTimestamp.put("include_lower", true);
        lowerRangeTimestamp.put("include_upper", true);
        lowerRange.put(timestamp, lowerRangeTimestamp);
        obj1.put("range", lowerRange);
        innerMust.add(obj1);

        JSONObject obj2 = new JSONObject();
        JSONObject upperRange = new JSONObject();
        JSONObject upperRangeTimestamp = new JSONObject();
        upperRangeTimestamp.put("from", null);
        upperRangeTimestamp.put("to", endTime);
        upperRangeTimestamp.put("include_lower", true);
        upperRangeTimestamp.put("include_upper", true);
        upperRange.put(timestamp, upperRangeTimestamp);
        obj2.put("range", upperRange);
        innerMust.add(obj2);

        innerBool.put("must", innerMust);
        outerMust.put("bool", innerBool);
        outerBool.put("must", outerMust);
        query.put("bool", outerBool);
        agg.put("query", query);

        return JSON.toJSONString(agg);
    }

    /**
     * this method is for low level rest client avg aggregation
     * request format is loke:
     * {
     *   "from": 0,
     *   "size": 0,
     *    "aggregations": {
     *      "AVG(score)": {
     *        "avg": {
     *          "field": "score"
     *        }
     *      }
     *    }
     *  }
     */
    public static String simpleAvgAggregation(int from, int hitSize, String field) {
        JSONObject agg = new JSONObject();
        agg.put("from", from);
        agg.put("size", hitSize);
        JSONObject aggregations = new JSONObject();
        JSONObject AVG = new JSONObject();
        JSONObject avg = new JSONObject();
        avg.put("field", field);
        AVG.put("avg", avg);
        aggregations.put("AVG(" + field + ")", AVG);
        agg.put("aggregations", aggregations);

        return JSON.toJSONString(agg);
    }

    public static String termAggregationWithOrder(int from, int hitSize, String aggName, String field,
                                                  int size, String orderBy, String order) {
        JSONObject agg = new JSONObject();
        agg.put("from", from);
        agg.put("size", hitSize);
        JSONObject aggregations = new JSONObject();
        JSONObject aggregationName = new JSONObject();
        JSONObject aggregationType = new JSONObject();
        JSONObject orderJson = new JSONObject();
        orderJson.put(orderBy, order);
        aggregationType.put("field", field);
        aggregationType.put("size", size);
        aggregationType.put("order", orderJson);
        aggregationName.put("terms", aggregationType);
        aggregations.put(aggName, aggregationName);
        agg.put("aggregations", aggregations);

        return JSON.toJSONString(agg);
    }

    public static String metricsAggregationWithTimerange(int from, int hitSize, String aggName, String aggType, String field,
                                                         int size, String timestamp, long startTime, long endTime,
                                                         String orderBy, String order) {
        JSONObject agg = new JSONObject();
        agg.put("from", from);
        agg.put("size", hitSize);

        // aggregation
        JSONObject aggField = new JSONObject();
        JSONObject aggregations = new JSONObject();
        JSONObject aggregationType = new JSONObject();
        JSONObject orderJson = new JSONObject();
        orderJson.put(orderBy, order);
        aggregationType.put("field", field);
        if (!aggType.equals("cardinality")) {
            aggregationType.put("size", size);
        }
        aggregationType.put("order", orderJson);
        aggField.put(aggType, aggregationType);
        aggregations.put(aggName, aggField);
        agg.put("aggregations", aggregations);

        // query
        JSONObject query = new JSONObject();
        JSONObject outerBool = new JSONObject();
        JSONObject outerMust = new JSONObject();
        JSONObject innerBool = new JSONObject();
        JSONArray innerMust = new JSONArray();

        JSONObject obj1 = new JSONObject();
        JSONObject lowerRange = new JSONObject();
        JSONObject lowerRangeTimestamp = new JSONObject();
        lowerRangeTimestamp.put("from", startTime);
        lowerRangeTimestamp.put("to", null);
        lowerRangeTimestamp.put("include_lower", true);
        lowerRangeTimestamp.put("include_upper", true);
        lowerRange.put(timestamp, lowerRangeTimestamp);
        obj1.put("range", lowerRange);
        innerMust.add(obj1);

        JSONObject obj2 = new JSONObject();
        JSONObject upperRange = new JSONObject();
        JSONObject upperRangeTimestamp = new JSONObject();
        upperRangeTimestamp.put("from", null);
        upperRangeTimestamp.put("to", endTime);
        upperRangeTimestamp.put("include_lower", true);
        upperRangeTimestamp.put("include_upper", true);
        upperRange.put(timestamp, upperRangeTimestamp);
        obj2.put("range", upperRange);
        innerMust.add(obj2);

        innerBool.put("must", innerMust);
        outerMust.put("bool", innerBool);
        outerBool.put("must", outerMust);
        query.put("bool", outerBool);
        agg.put("query", query);

        return JSON.toJSONString(agg);
    }

    public static String generate(int from, int hitSize, String aggregationBuilder, String query) {
        JSONObject res = new JSONObject();
        res.put("from", from);
        res.put("size", hitSize);

        res.put("aggregations", aggregationBuilder);
        res.put("query", query);

        return JSON.toJSONString(res);

    }
}
