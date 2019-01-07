package com.yifeng.restclient.utils.request_sender;

/**
 * Created by guoyifeng on 12/12/18
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Currently this class offers several methods aiming to remove incompatible fields on elasticsearch 2.x
 */
public class ElasticsearchFieldAdaptor {

    private static Logger LOG = LoggerFactory.getLogger(ElasticsearchFieldAdaptor.class);

    /**
     * delete 'boost' field from exists json in must not array
     * @param qb  querybuilder
     * @return JSONObject which has removed 'boost' field
     */
    public static JSONObject removeBoostInMustNot(QueryBuilder qb) {
        JSONObject res = JSON.parseObject(qb.toString());
        JSONArray mustNotArr = res.getJSONObject("bool").getJSONArray("must_not");
        for (int i = 0; i < mustNotArr.size(); i++) {
            if (mustNotArr.getJSONObject(i).containsKey("exists")) {
                res.getJSONObject("bool").getJSONArray("must_not").getJSONObject(i).getJSONObject("exists").remove("boost");  // exists is the n-th object in the array
            }
        }
        return res;
    }

    /**
     * overload of {@link ElasticsearchFieldAdaptor#removeBoostInMustNot(QueryBuilder qb)} <br>
     * used when multiple incompatible cases triggered
     * @param qbJson
     * @return
     */
    public static JSONObject removeBoostInMustNot(JSONObject qbJson) {
        JSONObject res = qbJson;
        JSONArray mustNotArr = res.getJSONObject("bool").getJSONArray("must_not");
        for (int i = 0; i < mustNotArr.size(); i++) {
            if (mustNotArr.getJSONObject(i).containsKey("exists")) {
                res.getJSONObject("bool").getJSONArray("must_not").getJSONObject(i).getJSONObject("exists").remove("boost");  // exists is the n-th object in the array
            }
        }
        return res;
    }

    /**
     * delete 'boost' field from exists json in must array
     * @param qb  querybuilder
     * @return JSONObject which has removed 'boost' field
     */
    public static JSONObject removeBoostInMust(QueryBuilder qb) {
        JSONObject res = JSON.parseObject(qb.toString());
        JSONArray mustArr = res.getJSONObject("bool").getJSONArray("must");
        for (int i = 0; i < mustArr.size(); i++) {
            if (mustArr.getJSONObject(i).containsKey("exists")) {
                res.getJSONObject("bool").getJSONArray("must").getJSONObject(i).getJSONObject("exists").remove("boost");  // exists is the n-th object in the array
            }
        }
        return res;
    }

    /**
     * overload of {@link ElasticsearchFieldAdaptor#removeBoostInMust(QueryBuilder qb)} <br>
     * used when multiple incompatible cases triggered
     * @param qbJson
     * @return
     */
    public static JSONObject removeBoostInMust(JSONObject qbJson) {
        JSONObject res = qbJson;
        JSONArray mustArr = res.getJSONObject("bool").getJSONArray("must");
        for (int i = 0; i < mustArr.size(); i++) {
            if (mustArr.getJSONObject(i).containsKey("exists")) {
                res.getJSONObject("bool").getJSONArray("must").getJSONObject(i).getJSONObject("exists").remove("boost");  // exists is the n-th object in the array
            }
        }
        return res;
    }

    /**
     * delete 'split_on_whitespace' field from query_string json in must not array
     * @param qb  querybuilder
     * @return JSONObject which has removed 'split_on_whitespace' field
     */
    public static JSONObject removeWhiteSpaceInMustNot(QueryBuilder qb) {
        JSONObject res = JSON.parseObject(qb.toString());
        JSONArray mustNotArr = res.getJSONObject("bool").getJSONArray("must_not");
        for (int i = 0; i < mustNotArr.size(); i++) {
            if (mustNotArr.getJSONObject(i).containsKey("query_string")) {
                res.getJSONObject("bool").getJSONArray("must_not").getJSONObject(i).getJSONObject("query_string").remove("split_on_whitespace");  // exists is the n-th object in the array
            }
        }
        return res;
    }

    /**
     * overload of {@link ElasticsearchFieldAdaptor#removeWhiteSpaceInMustNot(QueryBuilder qb)} <br>
     * used when multiple incompatible cases triggered
     * @param qbJson
     * @return
     */
    public static JSONObject removeWhiteSpaceInMustNot(JSONObject qbJson) {
        JSONObject res = qbJson;
        JSONArray mustNotArr = res.getJSONObject("bool").getJSONArray("must_not");
        for (int i = 0; i < mustNotArr.size(); i++) {
            if (mustNotArr.getJSONObject(i).containsKey("query_string")) {
                res.getJSONObject("bool").getJSONArray("must_not").getJSONObject(i).getJSONObject("query_string").remove("split_on_whitespace");  // exists is the n-th object in the array
            }
        }
        return res;
    }

    /**
     * delete 'split_on_whitespace' field from query_string json in must array
     * @param qb  querybuilder
     * @return JSONObject which has removed 'split_on_whitespace' field
     */
    public static JSONObject removeWhiteSpaceInMust(QueryBuilder qb) {
        JSONObject res = JSON.parseObject(qb.toString());
        JSONArray mustArr = res.getJSONObject("bool").getJSONArray("must");
        for (int i = 0; i < mustArr.size(); i++) {
            if (mustArr.getJSONObject(i).containsKey("query_string")) {
                res.getJSONObject("bool").getJSONArray("must").getJSONObject(i).getJSONObject("query_string").remove("split_on_whitespace");  // exists is the n-th object in the array
            }
        }
        return res;
    }

    /**
     * overload of {@link ElasticsearchFieldAdaptor#removeWhiteSpaceInMust(QueryBuilder qb)} <br>
     * used when multiple incompatible cases triggered
     * @param qbJson
     * @return
     */
    public static JSONObject removeWhiteSpaceInMust(JSONObject qbJson) {
        JSONObject res = qbJson;
        JSONArray mustArr = res.getJSONObject("bool").getJSONArray("must");
        for (int i = 0; i < mustArr.size(); i++) {
            if (mustArr.getJSONObject(i).containsKey("query_string")) {
                res.getJSONObject("bool").getJSONArray("must").getJSONObject(i).getJSONObject("query_string").remove("split_on_whitespace");  // exists is the n-th object in the array
            }
        }
        return res;
    }

    /**
     * delete 'split_on_whitespace' field from queryStringQuery
     * @param qb  querybuilder
     * @return JSONObject which has removed 'split_on_whitespace' field
     */
    public static JSONObject removeWhiteSpaceFromQueryString(QueryBuilder qb) {
        JSONObject res = JSON.parseObject(qb.toString());
        res.getJSONObject("query_string").remove("split_on_whitespace");
        return res;
    }

    /**
     * overload of {@link ElasticsearchFieldAdaptor#removeWhiteSpaceFromQueryString(QueryBuilder qb)} <br>
     * used when multiple incompatible cases triggered
     * @param qbJson
     * @return
     */
    public static JSONObject removeWhiteSpaceFromQueryString(JSONObject qbJson) {
        JSONObject res = qbJson;
        res.getJSONObject("query_string").remove("split_on_whitespace");
        return res;
    }
}
