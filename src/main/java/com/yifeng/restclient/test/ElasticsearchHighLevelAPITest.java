package com.yifeng.restclient.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonFactory;
import com.yifeng.restclient.config.AggregationRequestGenerator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.yifeng.restclient.config.DatasourceConstant.*;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

/**
 * Created by guoyifeng on 10/22/18
 */
public class ElasticsearchHighLevelAPITest {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchHighLevelAPITest.class);

    private static RestHighLevelClient restClient;

    @Before
    public void initial() {
        restClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ES_HOSTS, 29200, "http")));
    }

    @Test
    public void test1() {
        long startTime = System.currentTimeMillis();

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .size(10000)
                .query(QueryBuilders.matchAllQuery());

        SearchRequest searchRequest = new SearchRequest(UEBA_SETTINGS_INDEX)
                .types("user_info")
                .scroll(scroll)
                .source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            String scrollId = searchResponse.getScrollId();
            int counter = 0;
            while (hits != null && hits.length > 0) {
                for (SearchHit hit : hits) {
                    counter++;
                }
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = restClient.scroll(scrollRequest, RequestOptions.DEFAULT);
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
        Request request = new Request("POST", "/ueba_settings/user_info/_search");
        request.setEntity(new StringEntity(query));
        Response response = restClient.getLowLevelClient().performRequest(request);

        HttpEntity httpEntity = response.getEntity();
        System.out.print(EntityUtils.toString(httpEntity, "utf-8"));
    }

    @Test
    public void testTermsAggregation() throws IOException {
        String query = "{\"from\":0,\"size\":0,\"_source\":{\"includes\":[\"group\",\"COUNT\"],\"excludes\":[]},\"fields\":\"group\",\"aggregations\":{\"group\":{\"terms\":{\"field\":\"group\",\"size\":200},\"aggregations\":{\"group_member\":{\"value_count\":{\"field\":\"_index\"}}}}}}";
        Request request = new Request("POST", "/ueba_settings/user_info/_search");
        request.setEntity(new StringEntity(query));
        Response response = restClient.getLowLevelClient().performRequest(request);

        HttpEntity httpEntity = response.getEntity();
        System.out.print(EntityUtils.toString(httpEntity, "utf-8"));
    }

    @Test
    public void testRealAgg() throws IOException {
        restClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("172.16.150.189", 29200, "http")));
        String query = "{\"from\":0,\"size\":0,\"aggregations\":{\"user_name\":{\"terms\":{\"field\":\"user_name\",\"size\":1000}}}}";
        Request request = new Request("POST", "/event_*/event/_search");
        request.setEntity(new StringEntity(query));
        Response response = restClient.getLowLevelClient().performRequest(request);

        HttpEntity httpEntity = response.getEntity();
        System.out.print(EntityUtils.toString(httpEntity, "utf-8"));
    }

    @Test
    public void testSimpleTermAggregation() throws IOException {
        restClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("172.16.150.189", 29200, "http")));
        String query = AggregationRequestGenerator.simpleTermAggregation(0, 0, "term_name", "user_name", 1000);
        Request request = new Request("POST", "/event_*/event/_search");
        request.setEntity(new StringEntity(query));
        Response response = restClient.getLowLevelClient().performRequest(request);

        HttpEntity httpEntity = response.getEntity();
        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
        System.out.println(res);

//        String dsl = "{\"query\":{\"match_all\":{}}}";
//        System.out.println(JSON.parseObject(dsl));
//        query = AggregationRequestGenerator.termAggregationWithQuery(0, 0, "term_name", "user_name", 1000, dsl);
//        System.out.println(query);
    }

    @Test
    public void testListIndicesAPI() throws Exception {
        GetIndexRequest request = new GetIndexRequest().indices("*");
        GetIndexResponse response = restClient.indices().get(request, RequestOptions.DEFAULT);
        String[] indicesArr = response.getIndices();
        for (String s : indicesArr) {
            System.out.println(s);
        }
    }

    @Test
    public void testClusterHealthAPI() throws IOException {
        ClusterHealthResponse response = restClient.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        System.out.println(response.getNumberOfNodes());
    }

    @Test
    // get _mapping of es index
    public void testGetMappingAPI() throws IOException {
        GetMappingsResponse getMappingsResponse = restClient
                .indices()
                .getMapping(new GetMappingsRequest()
                        .indices("ueba_settings").types("user_info"), RequestOptions.DEFAULT);
        Iterator<ImmutableOpenMap<String, MappingMetaData>> it = getMappingsResponse.mappings().valuesIt();
        while (it.hasNext()) {
            ImmutableOpenMap<String, MappingMetaData> cur = it.next();
            MappingMetaData mappingMetaData = cur.get("user_info");
            Map<String, Object> propertiesMap = (Map) mappingMetaData.getSourceAsMap().get("properties");
            propertiesMap.forEach((k, v) -> {
                System.out.println(k + " " + v);
            });
        }
    }

    @Test
    public void testGetAvgScore() throws Exception {
        String query1 = "{\"from\":0,\"size\":0,\"aggregations\":{\"AVG(score)\":{\"avg\":{\"field\":\"score\"}}}}";
        String query2 = AggregationRequestGenerator.simpleAvgAggregation(0, 0, "score");
        Request request1 = new Request("POST", "/ueba_alarm/anomaly_scenarios/_search");
        request1.setEntity(new StringEntity(query1));
        Response response1 = restClient.getLowLevelClient().performRequest(request1);

        HttpEntity httpEntity1 = response1.getEntity();
        JSONObject res1 = JSON.parseObject(EntityUtils.toString(httpEntity1, "utf-8"));

        Thread.sleep(5000);
//
//        System.out.println(query2);
        Request request2 = new Request("POST", "/ueba_alarm/anomaly_scenarios/_search");
        request2.setEntity(new StringEntity(query2));
        Response response2 = restClient.getLowLevelClient().performRequest(request2);

        HttpEntity httpEntity2 = response2.getEntity();
        JSONObject res2 = JSON.parseObject(EntityUtils.toString(httpEntity2, "utf-8"));

        Assert.assertEquals(res1, res2);

    }

    @Test
    public void testTimerangeAgg() throws Exception {
        String query = AggregationRequestGenerator.metricsAggregationWithTimerange(0, 10, "termAgg", "terms", "user_name", 10, "occur_time", 1530400200000L, 1539334992074L);
        System.out.println(query);
//        String query = "{\"from\":0,\"size\":10,\"query\":{\"bool\":{\"must\":{\"bool\":{\"must\":[{\"range\":{\"occur_time\":{\"from\":1530400200000,\"to\":null,\"include_lower\":true,\"include_upper\":true}}},{\"range\":{\"occur_time\":{\"from\":null,\"to\":1539334992074,\"include_lower\":true,\"include_upper\":true}}}]}}}},\"aggregations\":{\"user_name\":{\"terms\":{\"field\":\"user_name\",\"size\":10}}}}";
        Request request = new Request("POST", "/ueba_alarm/anomaly_scenarios/_search");
        request.setEntity(new StringEntity(query));
        Response response = restClient.getLowLevelClient().performRequest(request);

        HttpEntity httpEntity = response.getEntity();
        System.out.print(EntityUtils.toString(httpEntity, "utf-8"));
    }

    @Test
    public void testTimerangeAggWithQuery() throws Exception {
        String query = AggregationRequestGenerator.metricsAggregationWithTimerange(0, 0 ,  "termAgg", "terms", "user_name",
                0, "occur_time", 1530400200000L, 1539334992074L,
                "entity_config_id", false);
        System.out.println(query);
        Request request = new Request("POST", "/ueba_alarm/anomaly_scenarios/_search");
        request.setEntity(new StringEntity(query));
        Response response = restClient.getLowLevelClient().performRequest(request);

        HttpEntity httpEntity = response.getEntity();
        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
        System.out.print(res);
    }

    @Test
    public void testCardinalityAgg() throws Exception {
        String  query = AggregationRequestGenerator.metricsAggregationWithTimerange(0, 0 ,  "cardinalityAgg", "cardinality", "user_name",
                0, "occur_time", 1530400200000L, 1539334992074L);
        System.out.println(query);
        Request request = new Request("POST", "/ueba_alarm/_search");
        request.setEntity(new StringEntity(query));
        Response response = restClient.getLowLevelClient().performRequest(request);

        HttpEntity httpEntity = response.getEntity();
        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
        Map<String, Long> map = new HashMap<>();
        long cardinality = (long) res.getJSONObject("aggregations").getJSONObject("cardinalityAgg").getLong("value");
        long total = (long) res.getJSONObject("hits").getLong("total");
        map.put("total", total);
        map.put("cardinality", cardinality);
        System.out.println(res);
        map.forEach((k, v) -> {
            System.out.println(k + " " + v);
        });
    }

    @Test
    public void testTermAggOrder() throws Exception {
        String query = AggregationRequestGenerator.metricsAggregationWithTimerange(0, 0, "field_value", "terms",
                "user_name", 5, "occur_time", 1530400200000L, 1539334992074L, "_count", "desc");
        System.out.println(query);
        Request request = new Request("POST", "/ueba_alarm/_search");
        request.setEntity(new StringEntity(query));
        Response response = restClient.getLowLevelClient().performRequest(request);

        HttpEntity httpEntity = response.getEntity();
        JSONObject res = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
        System.out.println(res);
    }

    @Test
    public void testQuery() throws Exception {
        QueryBuilder qb = QueryBuilders.rangeQuery("occur_time").to(1539334992074L).from(1530400200000L);
        System.out.println(((RangeQueryBuilder) qb).toString());
        System.out.println(JSONObject.parseObject(qb.toString()));
    }

    @Test
    public void general() throws Exception {
        String field = "user";
        int bucketSize = 5;
        String orderBy = "_count";
        String order = "desc";
        String[] includes = {"azh0595",
                "bcv0304",
                "ccm0822",
                "clr0460",
                "csc0581"};
        String[] excludes = {};
        JSONObject termsAgg = AggregationRequestGenerator.getTermsAgg(field, bucketSize, orderBy, order, includes, excludes);

        QueryBuilder qb = QueryBuilders.rangeQuery("occur_time").to(1535527697000L).from(1533108497000L);
        JSONObject query  = AggregationRequestGenerator.getQuery(qb);

        JSONObject dateAgg = AggregationRequestGenerator.getDateHistAgg("occur_time", "1d", "userCount", termsAgg);

        String res = AggregationRequestGenerator.dateHistogramAggregation(0, 0, query, "period", dateAgg);
        System.out.println(res);

        Request request = new Request("POST", "/saas_*/email/_search");
        request.setEntity(new StringEntity(res));
        Response response = restClient.getLowLevelClient().performRequest(request);

        HttpEntity httpEntity = response.getEntity();
        JSONObject result = JSON.parseObject(EntityUtils.toString(httpEntity, "utf-8"));
        System.out.println(result);

    }

    @Test
    public void testTopHits() {
//        TopHitsAggregationBuilder topHitsBuilder = AggregationBuilders.topHits("tophit_userid")
//                .fetchSource(true)
//                .size(1);
//        System.out.println(topHitsBuilder.toString());
        QueryBuilder q = QueryBuilders.matchAllQuery();
        JSONObject qb = AggregationRequestGenerator.getQuery(q);
        JSONObject topHits = AggregationRequestGenerator.getTopHitsAgg(0, 1, true);
        String query = AggregationRequestGenerator.userControllerAgg(0, 0, "user_id", 50000, "activity", "tophit_userid", topHits, qb);
        System.out.println(query);
    }

    @After
    public void close() throws IOException {
        restClient.close();
    }
}
