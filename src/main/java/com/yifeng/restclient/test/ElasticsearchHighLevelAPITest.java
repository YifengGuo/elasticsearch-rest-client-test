package com.yifeng.restclient.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.yifeng.restclient.config.DatasourceConstant.*;

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
        HttpGet getRequest = new HttpGet("http://172.16.150.149:29200");
        CloseableHttpResponse response = closeableHttpClient.execute(getRequest);
        String responseJSON = EntityUtils.toString(response.getEntity(), "utf-8");
//        System.out.print(responseJSON);
        JSONObject obj = JSONObject.parseObject(responseJSON);
//        System.out.print(obj);
        JSONObject version = obj.getJSONObject("version");
        String versionNumber = version.getString("number");
        System.out.print(versionNumber);
    }
}
