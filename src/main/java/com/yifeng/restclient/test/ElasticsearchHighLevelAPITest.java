package com.yifeng.restclient.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.yifeng.restclient.config.DatasourceConstant.*;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
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

    @After
    public void close() throws IOException {
        restClient.close();
    }
}
