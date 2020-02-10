package com.yifeng.restclient.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yifeng.restclient.config.AggregationRequestGenerator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.core.util.IOUtils;
import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import static com.yifeng.restclient.config.DatasourceConstant.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

/**
 * Created by guoyifeng on 10/22/18
 */
public class ElasticsearchHighLevelAPITest {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchHighLevelAPITest.class);

    private static RestHighLevelClient restClient;
    private static RestClient lowClient;

    private com.fasterxml.jackson.databind.ObjectMapper mapper = new ObjectMapper();

    @Before
    public void initial() {
        lowClient =  RestClient.builder(
                new HttpHost("172.16.150.184", 9200, "http")).build();
        restClient = new RestHighLevelClient(lowClient);
    }

    @Test
    public void test1() {
        long startTime = System.currentTimeMillis();

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .size(10000)
                .query(QueryBuilders.matchAllQuery());

        SearchRequest searchRequest = new SearchRequest("saas_demo_*")
                .types("email")
                .scroll(scroll)
                .source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = restClient.search(searchRequest);
            SearchHit[] hits = searchResponse.getHits().getHits();
            String scrollId = searchResponse.getScrollId();
            int counter = 0;
            while (hits != null && hits.length > 0) {
                for (SearchHit hit : hits) {
                    counter++;
                }
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = restClient.searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                hits = searchResponse.getHits().getHits();
            }
            System.out.println(counter);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }

        System.out.printf("It takes %.2f seconds to fetch user info data from elasticsearch", (System.currentTimeMillis() - startTime) / 1000.0);
    }

    /**
     * test http get request to fetch the version number of elasticsearch server
     * @throws Exception
     */
    @Test
    public void test2() throws Exception {
        CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build();
        HttpGet getRequest = new HttpGet("http" + File.pathSeparator + File.separator + File.separator + "172.16.150.149" + File.pathSeparator + 29200);
        CloseableHttpResponse response = closeableHttpClient.execute(getRequest);
        String responseJSON = EntityUtils.toString(response.getEntity(), "utf-8");
//        System.out.print(responseJSON);
        JSONObject obj = JSONObject.parseObject(responseJSON);
//        System.out.print(obj);
        JSONObject version = obj.getJSONObject("version");
        String versionNumber = version.getString("number");
        System.out.print(versionNumber.split("\\.")[0]);
    }

    /**
     * test rest client api aggregation usage on Elasticsearch 2.x
     */
    @Test
    public void testAvgAggregation() throws IOException {
        SearchRequest searchRequest = new SearchRequest("ueba_settings")
                .types("user_info")
                .source(new SearchSourceBuilder()
                    .query(boolQuery().must(matchQuery("mockup", "true")))
                    .aggregation(new AvgAggregationBuilder("first_opt_time").field("first_opt_time")));

        String query = "{\"from\":0,\"size\":0,\"query\":{\"bool\":{\"must\":{\"match\":{\"mockup\":{\"query\":\"true\",\"type\":\"phrase\"}}}}},\"_source\":{\"includes\":[\"AVG\"],\"excludes\":[]},\"aggregations\":{\"AVG(scenario_size)\":{\"avg\":{\"field\":\"scenario_size\"}}}}";
//        Request request = new Request("POST", "/ueba_settings/user_info/_search");
//        request.setEntity(new StringEntity(query));
        HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        Response response = lowClient.performRequest("POST", "/ueba_settings/user_info/_search", Collections.EMPTY_MAP, entity);

        HttpEntity httpEntity = response.getEntity();
        System.out.print(EntityUtils.toString(httpEntity, "utf-8"));
    }

    @Test
    public void testTermsAggregation() throws IOException {
        int topX = 5;
        String[] res = new String[topX];
        String query = AggregationRequestGenerator.termAggregationWithOrder(0, 0, "field_count", "user_account", topX, "_count", "desc");
        StringBuilder sb = new StringBuilder();
        sb.append(File.separator)
                .append("event_*")
                .append(File.separator)
                .append("event")
                .append(File.separator)
                .append("_search");
//            Request request = new Request("POST", sb.toString());
//            request.setEntity(new StringEntity(query));
        HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        Response response = lowClient.performRequest("POST", sb.toString(), Collections.emptyMap(), entity);
        HttpEntity httpEntity = response.getEntity();
        JSONObject responseJsonObj = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
        JSONObject agg = responseJsonObj.getJSONObject("aggregations");
        JSONObject fieldValue = agg.getJSONObject("field_count");
        JSONArray buckets = fieldValue.getJSONArray("buckets");
        for (int curIndex = 0; curIndex < buckets.size(); curIndex++) {
            if (curIndex == topX || curIndex == buckets.size()) {
                break;
            }
            res[curIndex] = buckets.getJSONObject(curIndex).getString("key");
        }
        for (String s : res) {
            System.out.println(s);
        }
    }
//
//    @Test
//    public void testRealAgg() throws IOException {
//        restClient = new RestHighLevelClient(
//                RestClient.builder(
//                        new HttpHost("172.16.150.189", 29200, "http")));
//        String query = "{\"from\":0,\"size\":0,\"aggregations\":{\"user_name\":{\"terms\":{\"field\":\"user_name\",\"size\":1000}}}}";
//        Request request = new Request("POST", "/event_*/event/_search");
//        request.setEntity(new StringEntity(query));
//        Response response = restClient.getLowLevelClient().performRequest(request);
//
//        HttpEntity httpEntity = response.getEntity();
//        System.out.print(EntityUtils.toString(httpEntity, "utf-8"));
//    }
//
//    @Test
//    public void testSimpleTermAggregation() throws IOException {
//        restClient = new RestHighLevelClient(
//                RestClient.builder(
//                        new HttpHost("172.16.150.189", 29200, "http")));
//        String query = AggregationRequestGenerator.simpleTermAggregation(0, 0, "term_name", "user_name", 1000);
//        Request request = new Request("POST", "/event_*/event/_search");
//        request.setEntity(new StringEntity(query));
//        Response response = restClient.getLowLevelClient().performRequest(request);
//
//        HttpEntity httpEntity = response.getEntity();
//        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
//        System.out.println(res);
//
////        String dsl = "{\"query\":{\"match_all\":{}}}";
////        System.out.println(JSON.parseObject(dsl));
////        query = AggregationRequestGenerator.termAggregationWithQuery(0, 0, "term_name", "user_name", 1000, dsl);
////        System.out.println(query);
//    }
//
//    @Test
//    public void testListIndicesAPI() throws Exception {
//        GetIndexRequest request = new GetIndexRequest().indices("*");
//        GetIndexResponse response = restClient.indices().get(request);
//        String[] indicesArr = response.getIndices();
//        for (String s : indicesArr) {
//            System.out.println(s);
//        }
//    }
//
//    @Test
//    public void testClusterHealthAPI() throws IOException {
//        ClusterHealthResponse response = restClient.cluster().health(new ClusterHealthRequest());
//        System.out.println(response.getNumberOfNodes());
//    }
//
//    @Test
//    // get _mapping of es index
//    public void testGetMappingAPI() throws IOException {
//        GetMappingsResponse getMappingsResponse = restClient
//                .indices()
//                .getMapping(new GetMappingsRequest()
//                        .indices("ueba_settings").types("user_info"));
//        Iterator<ImmutableOpenMap<String, MappingMetaData>> it = getMappingsResponse.mappings().valuesIt();
//        while (it.hasNext()) {
//            ImmutableOpenMap<String, MappingMetaData> cur = it.next();
//            MappingMetaData mappingMetaData = cur.get("user_info");
//            Map<String, Object> propertiesMap = (Map) mappingMetaData.getSourceAsMap().get("properties");
//            propertiesMap.forEach((k, v) -> {
//                System.out.println(k + " " + v);
//            });
//        }
//    }
//
//    @Test
//    public void testGetAvgScore() throws Exception {
//        String query1 = "{\"from\":0,\"size\":0,\"aggregations\":{\"AVG(score)\":{\"avg\":{\"field\":\"score\"}}}}";
//        String query2 = AggregationRequestGenerator.simpleAvgAggregation(0, 0, "score");
//        Request request1 = new Request("POST", "/ueba_alarm/anomaly_scenarios/_search");
//        request1.setEntity(new StringEntity(query1));
//        Response response1 = restClient.getLowLevelClient().performRequest(request1);
//
//        HttpEntity httpEntity1 = response1.getEntity();
//        JSONObject res1 = JSON.parseObject(EntityUtils.toString(httpEntity1, "utf-8"));
//
//        Thread.sleep(5000);
////
////        System.out.println(query2);
//        Request request2 = new Request("POST", "/ueba_alarm/anomaly_scenarios/_search");
//        request2.setEntity(new StringEntity(query2));
//        Response response2 = restClient.getLowLevelClient().performRequest(request2);
//
//        HttpEntity httpEntity2 = response2.getEntity();
//        JSONObject res2 = JSON.parseObject(EntityUtils.toString(httpEntity2, "utf-8"));
//
//        Assert.assertEquals(res1, res2);
//
//    }
//
//    @Test
//    public void testTimerangeAgg() throws Exception {
//        String query = AggregationRequestGenerator.metricsAggregationWithTimerange(0, 10, "termAgg", "terms", "user_name", 10, "occur_time", 1530400200000L, 1539334992074L);
//        System.out.println(query);
////        String query = "{\"from\":0,\"size\":10,\"query\":{\"bool\":{\"must\":{\"bool\":{\"must\":[{\"range\":{\"occur_time\":{\"from\":1530400200000,\"to\":null,\"include_lower\":true,\"include_upper\":true}}},{\"range\":{\"occur_time\":{\"from\":null,\"to\":1539334992074,\"include_lower\":true,\"include_upper\":true}}}]}}}},\"aggregations\":{\"user_name\":{\"terms\":{\"field\":\"user_name\",\"size\":10}}}}";
//        Request request = new Request("POST", "/ueba_alarm/anomaly_scenarios/_search");
//        request.setEntity(new StringEntity(query));
//        Response response = restClient.getLowLevelClient().performRequest(request);
//
//        HttpEntity httpEntity = response.getEntity();
//        System.out.print(EntityUtils.toString(httpEntity, "utf-8"));
//    }
//
    @Test
    public void testTimerangeAggWithQuery() throws Exception {
        String query = AggregationRequestGenerator.metricsAggregationWithTimerange(0, 10, "agg", "terms",
                "entity", 10, "occur_time", 1341001600633L, 1543593599999L, "entity_config_id", true);
        StringBuilder sb = new StringBuilder();
        sb.append(File.separator)
                .append(UEBA_ALARM_INDEX)
                .append(File.separator)
                .append("anomaly_scenarios")
                .append(File.separator)
                .append("_search");
        System.out.println(query);
//                Request request = new Request("POST", sb.toString());
//                request.setEntity(new StringEntity(query));
        HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        Response response = lowClient.performRequest("POST", sb.toString(), Collections.EMPTY_MAP, entity);
        HttpEntity httpEntity = response.getEntity();
        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
        System.out.println(res);
    }
//
//    @Test
//    public void testCardinalityAgg() throws Exception {
//        String  query = AggregationRequestGenerator.metricsAggregationWithTimerange(0, 0 ,  "cardinalityAgg", "cardinality", "user_name",
//                0, "occur_time", 1530400200000L, 1539334992074L);
//        System.out.println(query);
//        Request request = new Request("POST", "/ueba_alarm/_search");
//        request.setEntity(new StringEntity(query));
//        Response response = restClient.getLowLevelClient().performRequest(request);
//
//        HttpEntity httpEntity = response.getEntity();
//        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
//        Map<String, Long> map = new HashMap<>();
//        long cardinality = (long) res.getJSONObject("aggregations").getJSONObject("cardinalityAgg").getLong("value");
//        long total = (long) res.getJSONObject("hits").getLong("total");
//        map.put("total", total);
//        map.put("cardinality", cardinality);
//        System.out.println(res);
//        map.forEach((k, v) -> {
//            System.out.println(k + " " + v);
//        });
//    }
//
//    @Test
//    public void testTermAggOrder() throws Exception {
//        String query = AggregationRequestGenerator.metricsAggregationWithTimerange(0, 0, "field_value", "terms",
//                "user_name", 5, "occur_time", 1530400200000L, 1539334992074L, "_count", "desc");
//        System.out.println(query);
//        Request request = new Request("POST", "/ueba_alarm/_search");
//        request.setEntity(new StringEntity(query));
//        Response response = restClient.getLowLevelClient().performRequest(request);
//
//        HttpEntity httpEntity = response.getEntity();
//        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
//        System.out.println(res);
//    }
//
//    @Test
//    public void testQuery() throws Exception {
//        QueryBuilder qb = QueryBuilders.rangeQuery("occur_time").to(1539334992074L).from(1530400200000L);
//        System.out.println(((RangeQueryBuilder) qb).toString());
//        System.out.println(JSONObject.parseObject(qb.toString()));
//    }
//
//    @Test
//    public void general() throws Exception {
//        String field = "user";
//        int bucketSize = 5;
//        String orderBy = "_count";
//        String order = "desc";
//        String[] includes = {"azh0595",
//                "bcv0304",
//                "ccm0822",
//                "clr0460",
//                "csc0581"};
//        String[] excludes = {};
//        JSONObject termsAgg = AggregationRequestGenerator.getTermsAgg(field, bucketSize, orderBy, order, includes, excludes);
//
//        QueryBuilder qb = QueryBuilders.rangeQuery("occur_time").to(1535527697000L).from(1533108497000L);
//        JSONObject query  = AggregationRequestGenerator.getQuery(qb);
//
//        JSONObject dateAgg = AggregationRequestGenerator.getDateHistAgg("occur_time", "1d", "userCount", termsAgg);
//
//        String res = AggregationRequestGenerator.dateHistogramAggregation(0, 0, query, "period", dateAgg);
//        System.out.println(res);
//
//        Request request = new Request("POST", "/saas_*/email/_search");
//        request.setEntity(new StringEntity(res));
//        Response response = restClient.getLowLevelClient().performRequest(request);
//
//        HttpEntity httpEntity = response.getEntity();
//        JSONObject result = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
//        System.out.println(result);
//
//    }
//
//    @Test
//    public void testTopHits() {
////        TopHitsAggregationBuilder topHitsBuilder = AggregationBuilders.topHits("tophit_userid")
////                .fetchSource(true)
////                .size(1);
////        System.out.println(topHitsBuilder.toString());
//        QueryBuilder q = QueryBuilders.matchAllQuery();
//        JSONObject qb = AggregationRequestGenerator.getQuery(q);
//        JSONObject topHits = AggregationRequestGenerator.getTopHitsAgg(0, 1, true);
//        String query = AggregationRequestGenerator.userControllerAgg(0, 0, "user_id", 50000, "activity", "tophit_userid", topHits, qb);
//        System.out.println(query);
//    }
//
//    @Test
//    public void testSingleValueAgg() throws Exception {
//        JSONObject agg = AggregationRequestGenerator.getSingleValueAggregation(0, 0, "min_ts", "min", "occur_time");
//        String query = JSON.toJSONString(agg);
//        Request request = new Request("POST", "/saas_20180801/_search");
//        request.setEntity(new StringEntity(query));
//        Response response = restClient.getLowLevelClient().performRequest(request);
//
//        HttpEntity httpEntity = response.getEntity();
//        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
//        System.out.println(res);
//    }

    @Test
    public void testGetMappingByLowLevel() throws IOException {
        Response response = lowClient.performRequest("GET", "/event_2018*/event/_mapping");
        HttpEntity httpEntity = response.getEntity();
        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
        System.out.println(res);
    }

    @Test
    public void testGetIndicesByLowLevel() throws IOException {
        Response response = lowClient.performRequest("GET", "/_cat/indices?v");
        HttpEntity httpEntity = response.getEntity();
//        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
        BufferedReader br = new BufferedReader(new InputStreamReader(httpEntity.getContent()));
        String line = "";
        while ((line = br.readLine()) != null) {
            System.out.println(line.split("\\s+")[2]);
        }
//            System.out.println(httpEntity.getContent());
    }

    @Test
    public void testExistIndexByLowLevel() throws IOException {
        Response response = lowClient.performRequest("HEAD", "event_*");
        System.out.println(response.getStatusLine().getStatusCode());
    }

    @Test
    public void testClusterHealthByLowLevel() throws IOException {
        Response response = lowClient.performRequest("GET", "/_cat/health?v");
        HttpEntity httpEntity = response.getEntity();
        BufferedReader br = new BufferedReader(new InputStreamReader(httpEntity.getContent()));
        String line = "";
        while ((line = br.readLine()) != null) {
//            System.out.println(line.split("\\s+")[4]);
            System.out.println(line);

        }

    }

    @Test
    public void deleteDocuments() throws IOException {
        QueryBuilder qb = QueryBuilders.boolQuery().must(rangeQuery("score").gt(0));
//        QueryBuilder qb = QueryBuilders.matchAllQuery();
        String targetIndex = "ueba_settings";
        String targetType = "user_info";
        SearchResponse response = restClient.search(new SearchRequest(targetIndex).types(targetType)
        .scroll(new Scroll(TimeValue.timeValueMillis(60000L)))
        .source(new SearchSourceBuilder()
            .query(qb)
            .size(10000)));
        System.out.println(response.getHits().getTotalHits());
        while (true) {
            BulkRequest bulkRequest = new BulkRequest();
            for (SearchHit hit : response.getHits().getHits()) {
                bulkRequest.add(new DeleteRequest(targetIndex).type(hit.getType()).id(hit.getId()));
            }
            restClient.bulk(bulkRequest);
            response = restClient.searchScroll(new SearchScrollRequest(response.getScrollId()).scroll(TimeValue.timeValueMillis(60000L)));
            if (response.getHits().getHits().length == 0) {
                break;
            }
        }
        System.out.println("deletion complete");
    }

    @Test
    public void updateFields() throws IOException {
        Map<String, Long> map = new HashMap<>();
        map.put("update_frequency", 86400000L);
        restClient.update(new UpdateRequest("ueba_settings", "user_config", "AWh4gABIO2i96pokHhma").doc(map).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
    }

    @Test
    public void modifyExistsQuery() {
        BoolQueryBuilder userQb = boolQuery().mustNot(termsQuery("id", "xx")).mustNot(
                (existsQuery("user_config_id")));
        System.out.println(userQb);
        String s = userQb.toString();
        JSONObject obj = JSON.parseObject(s);
        obj.getJSONObject("bool").getJSONArray("must_not").getJSONObject(1).getJSONObject("exists").remove("boost");
        System.out.println(obj);
    }

    @Test
    public void testWatchUser() throws IOException {
        boolean isEntity = true;
        boolean isWatch = true;
        String configId = "6c94c280-ee3d-4535-a4f5-b83d8719f397";
        String id = "tuobabingzhen";

        BoolQueryBuilder qb = boolQuery().must(matchQuery("id", id));
        if (isEntity) {
            qb = qb.must(matchQuery("entity_config_id", configId));
        } else {
            qb = qb.must(matchQuery("user_config_id", configId));
        }
        SearchResponse sr = restClient.search(new SearchRequest("ueba_settings")
                .types("user_info")
                .source(new SearchSourceBuilder()
                        .query(qb)));
        HashMap<String, Object> map = new HashMap<>();
        map.put("is_watch", isWatch);
//        System.out.println(sr);

        if (sr.getHits().getTotalHits() > 0) {
            UpdateResponse updateResponse = restClient.update(new UpdateRequest("ueba_settings",
                    "user_info", sr.getHits().getAt(0).getId())
                    .doc(map).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
        }
    }

//    @Test
//    public void testDeleteDatasource() throws IOException {
//        SearchRequest request = new SearchRequest("ueba_settings")
//                .types("datasource")
//                .source(new SearchSourceBuilder()
//                        .query(matchQuery("id", "d2a22d1c-671d-4819-945f-0864bfe1f9b1"))
//                        .size(100));
//        SearchResponse response = restClient.search(request);
////        System.out.println(request.toString());
////        System.out.println(response);
////        System.out.println(response.getHits().getTotalHits());
//        DeleteResponse deleteResponse = restClient.delete(new DeleteRequest("ueba_settings", "datasource", response.getHits().getAt(0).getId())
//                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
//        System.out.println(deleteResponse.status());
//    }

    @Test
    public void testWrappQuery() throws Exception {
        QueryBuilder query = QueryBuilders.matchAllQuery();
//        QueryBuilder qb = QueryBuilders.boolQuery()
//                .must(query)
//                .must(QueryBuilders.rangeQuery("occur_time").gte(0).lt(10));
//        JSONObject qbJson = JSON.parseObject(qb.toString());
//        JSONArray must = qbJson.getJSONObject("bool").getJSONArray("must");
//        for (int i = 0; i < must.size(); i++) {
//            if (must.getJSONObject(i).containsKey("query_string")) {
//                qbJson.getJSONObject("bool").getJSONArray("must").getJSONObject(i).getJSONObject("query_string").remove("split_on_whitespace");
//            }
//        }
        System.out.println(QueryBuilders.wrapperQuery(query.toString()));
        SearchResponse response = restClient.search(new SearchRequest("ueba_alarm")
            .types("anomaly_scenarios")
            .source(new SearchSourceBuilder()
                .query(QueryBuilders.wrapperQuery(query.toString()))
                .size(1)));
        System.out.println(response.getHits().getHits()[0].getSourceAsMap());
    }

    @Test
    public void testQueryStringQuery() throws Exception {
        QueryBuilder qb = QueryBuilders.queryStringQuery("_exists_:scenarios_top3");
        System.out.println(qb);
    }

    @Test
    public void testArrayElementsQuery() throws Exception {
        List<Long> target = Arrays.asList(168430090L, 168430180L);
        BoolQueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("category_id", "75aa33dc-943a-4d2c-9799-6c51725137eb"))
                .must(QueryBuilders.termQuery("type", "IP_RANGE"))
                .must(QueryBuilders.termQuery("target", target.get(0)))
                .must(QueryBuilders.termQuery("target", target.get(1)));
        SearchResponse response = restClient.search(new SearchRequest("ueba_settings")
                .types(BLACKLIST_TYPE)
                .source(new SearchSourceBuilder()
                        .query(qb)
                        .size(1000)));
        System.out.println(response.getHits().getHits()[0].getSourceAsMap());
    }

    @Test
    public void t() {
        LOG.info("1");
    }

    @After
    public void close() throws IOException {
        lowClient.close();
    }
}
