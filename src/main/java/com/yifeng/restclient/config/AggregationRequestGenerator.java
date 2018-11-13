package com.yifeng.restclient.config;

/**
 * Created by guoyifeng on 10/25/18
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * this class is to supply several common aggregation wrap methods for elasticsearch low level rest client
 */
public class AggregationRequestGenerator {

    /**
     * this method is for simple term aggregation
     * request format is like:
     * {
     *   "from": 0,  // the offset from the first result you want to fetch
     *   "size": 0,  // configure the maximum amount of hits to be returned in hits[]
     *   "aggregations": {
     *     "user_name": {
     *       "terms": {
     *         "field": "user_name",
     *         "size": 1000   // the size of terms to be returned in the corresponding bucket
     *       }
     *     }
     *   }
     * }
     * @return json string as query for the aggregation
     * eg. "{\"from\":0,\"size\":0,\"_source\":{\"includes\":[\"group\",\"COUNT\"],\"excludes\":[]},
     *      \"fields\":\"group\",\"aggregations\":{\"group\":{\"terms\":{\"field\":\"group\",\"size\":200},
     *      \"aggregations\":{\"group_member\":{\"value_count\":{\"field\":\"_index\"}}}}}}";
     */
    public static String simpleTermAggregation(int from, int hitSize, String aggName, String field, int size) {
        JSONObject agg = new JSONObject();
        agg.put("from", from);
        agg.put("size", hitSize);
        JSONObject aggregations = new JSONObject();
        JSONObject aggregationName = new JSONObject();
        JSONObject aggregationType = new JSONObject();
        aggregationType.put("field", field);
        aggregationType.put("size", size);
        aggregationName.put("terms", aggregationType);
        aggregations.put(aggName, aggregationName);
        agg.put("aggregations", aggregations);

        return JSON.toJSONString(agg);
    }

    public static String termAggregationWithQuery(int from, int hitSize, String aggName, String field, int size, String dsl) {
        JSONObject agg = new JSONObject();
        agg.put("from", from);
        agg.put("size", hitSize);
        JSONObject aggregations = new JSONObject();
        JSONObject aggregationName = new JSONObject();
        JSONObject aggregationType = new JSONObject();
        aggregationType.put("field", field);
        aggregationType.put("size", size);
        aggregationName.put("terms", aggregationType);
        aggregations.put(aggName, aggregationName);
        agg.put("aggregations", aggregations);

        JSONObject dslObj = JSON.parseObject(dsl);
        agg.put("query", dslObj.get("query"));

        return JSON.toJSONString(agg);
    }

    /**
     * @param orderBy the key which the bucket will be sorted by. eg: "_count", "_key", "_term" etc
     * @param order the order in which terms will be sorted. "desc" or "asc"
     */
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

    /**
     * {
     *   "from": 0,
     *   "size": 10,
     *   "query": {
     *     "bool": {
     *       "must": {
     *         "bool": {
     *           "must": [
     *             {
     *               "range": {
     *                 "occur_time": {
     *                   "from": 1530400200000,
     *                   "to": null,
     *                   "include_lower": true,
     *                   "include_upper": true
     *                 }
     *               }
     *             },
     *             {
     *               "range": {
     *                 "occur_time": {
     *                   "from": null,
     *                   "to": 1539334992074,
     *                   "include_lower": true,
     *                   "include_upper": true
     *                 }
     *               }
     *             },
     *             {
     *               "bool": {
     *                 "must_not": {
     *                   "missing": {
     *                     "field": "entity_config_id"
     *                   }
     *                 }
     *               }
     *             }
     *           ]
     *         }
     *       }
     *     }
     *   },
     *   "aggregations": {
     *     "user_name": {
     *       "terms": {
     *         "field": "user_name",
     *         "size": 10
     *       }
     *     }
     *   }
     * }
     * @param from
     * @param hitSize
     * @param field agg by this field
     * @param aggType the type of metrics aggregation. eg: "terms", "cardinality", etc
     * @param size the size of terms to be returned in the corresponding bucket
     * @param timestamp field name of timestamp like "occur_time"
     * @param startTime
     * @param endTime
     * @param flagField additional bool query if some field is existed
     * @param isExisted
     * @return
     */
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
            JSONObject bool = new JSONObject();
            JSONObject mustNot = new JSONObject();
            JSONObject missing = new JSONObject();
            missing.put("field", flagField);
            mustNot.put("missing", missing);
            bool.put("must_not", mustNot);
            obj3.put("bool", bool);
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


    // overload method where buckets need sort
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


    /****************************      date histogram aggregation with terms sub-aggregation      ********************/
    public static String dateHistogramAggregation(int from, int hitSize, JSONObject query, String dateHistAggName, JSONObject dateHistAgg) {
        JSONObject res = new JSONObject();
        res.put("from", from);
        res.put("size", hitSize);
        res.put("query", query);
        JSONObject aggregations = new JSONObject();
        aggregations.put(dateHistAggName, dateHistAgg);
        res.put("aggregations", aggregations);
        return JSON.toJSONString(res);
    }

    // a common method to get QueryBuilder as JSONObject
    public static JSONObject getQuery(QueryBuilder qb) {
        return JSONObject.parseObject(qb.toString());
    }


    public static JSONObject getDateHistAgg(String field, String interval, String termsAggName, JSONObject termsAgg) {
        JSONObject res = new JSONObject();
        JSONObject dateAgg = new JSONObject();
        dateAgg.put("field", field);
        dateAgg.put("interval", interval);
        res.put("date_histogram", dateAgg);
        // here we need to add subaggregation under dateHistAggName
        JSONObject termJson = new JSONObject();
        termJson.put(termsAggName, termsAgg);
        res.put("aggregations", termJson);
        return res;
    }

    // currently just consider one sort order
    // a common method to get terms aggregation JSONObject
    public static JSONObject getTermsAgg(String field, int bucketSize, String orderBy, String order, String[] include, String[] exclude) {
        JSONObject res = new JSONObject();
        JSONObject terms = new JSONObject();
        terms.put("field", field);
        terms.put("size", bucketSize);
        JSONObject orderJson = new JSONObject();
        orderJson.put(orderBy, order);
        terms.put("order", orderJson);
        terms.put("include", include);
        terms.put("exclude", exclude);
        res.put("terms", terms);
        return res;
    }
    /****************************      date histogram aggregation with terms sub-aggregation      ********************/


    /****************************      user controller aggregation use case                       ********************/

    // a common method to get top hits aggregation JSONObject
    public static JSONObject getTopHitsAgg(int from, int hitSize, boolean fetchSource) {
        JSONObject res = new JSONObject();
        JSONObject topHits = new JSONObject();
        topHits.put("from", from);
        topHits.put("size", hitSize);
        topHits.put("_source", fetchSource);
        res.put("top_hits", topHits);
        return res;
    }

    public static String userControllerAgg(int from, int hitSize, String aggName, int bucketSize, String field, String subAggName, JSONObject subAgg, JSONObject query) {
        JSONObject res = new JSONObject();
        res.put("query", query);
        res.put("from", from);
        res.put("size", hitSize);
        JSONObject aggregations = new JSONObject();
        JSONObject parent = new JSONObject();
        JSONObject terms = new JSONObject();
        terms.put("field", field);
        terms.put("size", bucketSize);
        parent.put("terms", terms);

        // add sub aggregation
        JSONObject subAggJson = new JSONObject();
        subAggJson.put(subAggName, subAgg);
        parent.put("aggregations", subAggJson);
        aggregations.put(aggName, parent);
        res.put("aggregations", aggregations);

        return JSON.toJSONString(res);
    }


    // get single value aggregation request json including max, min, avg...
    public static JSONObject getSingleValueAggregation(int from, int hitSize, String aggName, String aggType, String field) {
        JSONObject res = new JSONObject();
        res.put("from", from);
        res.put("size", hitSize);
        JSONObject aggregations = new JSONObject();
        JSONObject name = new JSONObject();
        JSONObject type = new JSONObject();
        type.put("field", field);
        name.put(aggType, type);
        aggregations.put(aggName, name);
        res.put("aggregations", aggregations);
        return res;
    }
}
