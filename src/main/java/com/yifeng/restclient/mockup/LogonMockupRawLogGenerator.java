package com.yifeng.restclient.mockup;

/**
 * Created by guoyifeng on 1/5/19
 */

import com.alibaba.fastjson.JSONObject;
import com.yifeng.restclient.config.ElasticsearchConnection;
import com.yifeng.restclient.utils.request_sender.CustomAggregationBuilder;
import com.yifeng.restclient.utils.request_sender.ElasticsearchRequestSender;
import com.yifeng.restclient.utils.request_sender.ElasticsearchRequestSenderFactory;
import com.yifeng.restclient.utils.request_sender.NewAggregationRequestGenerator;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * 登录事件原始数据生成器
 */
public class LogonMockupRawLogGenerator extends BaseMockupRawLog {

    private static Logger LOG = LoggerFactory.getLogger(LogonMockupRawLogGenerator.class);

    private ElasticsearchConnection connection;

    private static String currIndex;

    private String targetName = "shengbingshuang";

    private Date currDay;

    public LogonMockupRawLogGenerator(ElasticsearchConnection connection, Date currDay) {
        this.connection = connection;
        this.currDay = currDay;
        long tmp = randamThatDay(currDay);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String s = sdf.format(tmp);
        currIndex = "event_" + s;
        LOG.info("currIndex is {}", currIndex);
    }

    public SearchResponse getHistoryDocs(String eventName) {
        try {
            QueryBuilder qb = QueryBuilders.boolQuery()
                    .must(matchQuery("user_name", "quanwu"))
//                    .must(matchAllQuery())
//                    .must(matchQuery("event_name", eventName))
//                    .must(rangeQuery("occur_time").gte(getPreviousMonthMS()).lte(System.currentTimeMillis()))
            ;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            SearchResponse response = connection.client().search(new SearchRequest("event_" + simpleDateFormat.format(currDay))
                    .types("event")
                    .source(new SearchSourceBuilder()
                            .query(qb)
                            .size(10000)));
            return response;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void generateData(SearchResponse response) {
        try {
            if (response == null || response.getHits().getTotalHits() == 0) return;
            BulkRequest bulkRequest = new BulkRequest();
            for (SearchHit hit : response.getHits().getHits()) {
                Map<String, Object> currMap = hit.getSourceAsMap();
                currMap.put("occur_time", randamThatDay(currDay));
                bulkRequest.add(new IndexRequest(currIndex).type("event").source(currMap)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            connection.client().bulk(bulkRequest);
            System.out.println("done");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void insertLogonFailureMockupData() {
        if (currIndex == null || currIndex.isEmpty()) return;
        try {
            SearchResponse response = connection.client().search(new SearchRequest(currIndex)
                .types("event")
                .source(new SearchSourceBuilder()
                    .query(boolQuery().must(matchQuery("event_name", "登录事件")).must(matchQuery("result", "/fail")))
                    .size(1)));
            Map<String, Object> currMap = response.getHits().getHits()[0].getSourceAsMap();
            LOG.info("登录次数过多mock username {}", currMap.get("user_name"));
            int randomCount = ThreadLocalRandom.current().nextInt(5, 30);
            long timestamp = (long) currMap.get("occur_time");
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < randomCount; i++) {
                currMap.put("occur_time", timestamp + ThreadLocalRandom.current().nextLong(0, 60 * 60 * 1000));  // randomly add time within one hour to satisfy incident configuration
                bulkRequest.add(new IndexRequest(currIndex).type("event").source(currMap));
            }
            connection.client().bulk(bulkRequest);
            LOG.info("登录失败次数过多数据插入完成");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * basic insert logic
     * 1. terms aggs on user_name and do sub-aggs on result to find which user has max successful log count
     * 2. insert mockup data on this user and now all logon result is fail
     */
    public void insertLogonCountDeviateBaselineMockupData() {
        if (currIndex == null || currIndex.isEmpty()) return;
        ElasticsearchRequestSender sender = ElasticsearchRequestSenderFactory.createSender(connection);
        StringBuilder endpoint = new StringBuilder();
        endpoint.append('/')
                .append(currIndex)
                .append('/')
                .append("event")
                .append('/')
                .append("_search");
        JSONObject res = sender.aggregation(new NewAggregationRequestGenerator().size(0).aggregation(new CustomAggregationBuilder()
                .setAggName("name1")
                .setAggType("terms")
                .setFieldName("user_name")
                .setSubaggregation(new CustomAggregationBuilder()
                        .setAggName("name2")
                        .setAggType("terms")
                        .setFieldName("result")
                        .setOrder(new JSONObject(), obj -> obj.put("_count", "desc")))
                .complete()
                .complete()))
                .query(QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchQuery("result", "/success"))
                    .must(matchQuery("user_name", targetName)))
                .performRequest("POST", endpoint.toString(), Collections.EMPTY_MAP, sender.getRequestEntity());

        // get the first user who has max success logon count
        String userName = res.getJSONObject("aggregations").getJSONObject("name1").getJSONArray("buckets").getJSONObject(0).getString("key");
        int docCount = res.getJSONObject("aggregations").getJSONObject("name1").getJSONArray("buckets").getJSONObject(0).getInteger("doc_count");
        LOG.info("基线username是{} ", userName);

        try {
            SearchResponse response = connection.client().search(new SearchRequest(currIndex).types("event").source(new SearchSourceBuilder()
                    .query(boolQuery().must(matchQuery("user_name", userName)).must(matchQuery("result", "/success")))));
            if (response.getHits().getTotalHits() == 0) {
                throw new NoSuchElementException("cannot find proper document");
            }
            Map<String, Object> currMap = response.getHits().getHits()[0].getSourceAsMap();
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < ThreadLocalRandom.current().nextInt(docCount + 5, docCount + 25); i++) {
                currMap.put("occur_time", randamThatDay(currDay));
                bulkRequest.add(new IndexRequest(currIndex).type("event").source(currMap));
            }
            connection.client().bulk(bulkRequest);
            LOG.info("登录次数偏离基线数据插入完成");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * basic insert logic
     * 1. terms aggs on user_name and do sub-aggs on src_city to find which user has max log location
     * 2. insert mockup data on this user and now change the logon src_city
     */
    public void insertAbnormalLogonLocation() {

        List<String> cities = Arrays.asList("北京", "上海", "广州", "深圳", "珠海", "成都", "杭州", "南京", "苏州", "大连");

        if (currIndex == null || currIndex.isEmpty()) return;
        ElasticsearchRequestSender sender = ElasticsearchRequestSenderFactory.createSender(connection);
        StringBuilder endpoint = new StringBuilder();
        endpoint.append('/')
                .append(currIndex)
                .append('/')
                .append("event")
                .append('/')
                .append("_search");
        JSONObject res = sender.aggregation(new NewAggregationRequestGenerator().size(0).aggregation(new CustomAggregationBuilder()
                .setAggName("name1")
                .setAggType("terms")
                .setFieldName("user_name")
                .setSubaggregation(new CustomAggregationBuilder()
                        .setAggName("name2")
                        .setAggType("terms")
                        .setFieldName("src_city")
                        .setOrder(new JSONObject(), obj -> obj.put("_count", "desc")))
                .complete()
                .complete()))
                .query(QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("result", "/success")))
                .performRequest("POST", endpoint.toString(), Collections.EMPTY_MAP, sender.getRequestEntity());

        String userName = res.getJSONObject("aggregations").getJSONObject("name1").getJSONArray("buckets").getJSONObject(0).getString("key");
        String baseCity = res.getJSONObject("aggregations").getJSONObject("name1").getJSONArray("buckets").getJSONObject(0).getJSONObject("name2").getJSONArray("buckets").getJSONObject(0).getString("key");
        LOG.info("异常登录用户{} ", userName);
        LOG.info("常用登录地{} ", baseCity);

        try {
            SearchResponse response = connection.client().search(new SearchRequest(currIndex).types("event").source(new SearchSourceBuilder()
                    .query(boolQuery().must(matchQuery("user_name", userName)).must(matchQuery("result", "/success")))));
            if (response.getHits().getTotalHits() == 0) {
                throw new NoSuchElementException("cannot find proper document");
            }
            Map<String, Object> currMap = response.getHits().getHits()[0].getSourceAsMap();
            BulkRequest bulkRequest = new BulkRequest();
            String currCity;
            while (true) {
                currCity = cities.get(ThreadLocalRandom.current().nextInt(0, cities.size()));
                if (!currCity.equals(baseCity)) break;
            }
            LOG.info("mockup登录地{} ", currCity);
            for (int i = 0; i < ThreadLocalRandom.current().nextInt( 5,  10); i++) {
                currMap.put("src_city", currCity);
                currMap.put("occur_time", randamThatDay(currDay));
                bulkRequest.add(new IndexRequest(currIndex).type("event").source(currMap));
            }
            connection.client().bulk(bulkRequest);
            LOG.info("登录地偏离基线数据插入完成");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        ElasticsearchConnection connection;
        try {
            connection = new ElasticsearchConnection();
            connection.connect("172.16.150.189", 29200, "http");
//            for (LocalDate date = LocalDate.of( 2018 , Month.DECEMBER , 15); date.isBefore(LocalDate.of(2019, Month.JANUARY, 6)); date = date.plusDays(1)) {
//                LogonMockupRawLogGenerator generator = new LogonMockupRawLogGenerator(connection, asDate(date));
//                SearchResponse response = generator.getHistoryDocs("");
//                generator.generateData(response);
//                generator.insertLogonFailureMockupData();
//                generator.insertLogonCountDeviateBaselineMockupData();
//                generator.insertAbnormalLogonLocation();
//                LOG.info(date + " complete");
//            }

            for (LocalDate date = LocalDate.of( 2019 , Month.JANUARY , 1); date.isBefore(LocalDate.of(2019, Month.JANUARY, 6)); date = date.plusDays(1)) {
                LogonMockupRawLogGenerator generator = new LogonMockupRawLogGenerator(connection, asDate(date));
                generator.insertLogonCountDeviateBaselineMockupData();
                LOG.info(date + " complete");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
