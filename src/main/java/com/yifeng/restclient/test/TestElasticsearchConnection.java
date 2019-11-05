package com.yifeng.restclient.test;

import com.yifeng.restclient.config.ElasticsearchConnection;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

/**
 * Created by guoyifeng on 11/3/18
 */
public class TestElasticsearchConnection {
    @Test
    public void testEsConn() throws IOException {
//        ElasticsearchConnection connection = new ElasticsearchConnection();
//        connection.connect("172.16.150.149", 29200, "http");
        SearchResponse response = null;
        String id = "OEH0380";
        String configId = "da272dbd-a650-42ce-9ff1-38c350eaf4d9";
        boolean isEntity = false;
//        BoolQueryBuilder qb = boolQuery().must(QueryBuilders.boolQuery()
//                .should(matchQuery("id", id))
//                .should(matchQuery("id", id.toLowerCase())));
//        if (isEntity) {
//            qb = qb.must(matchQuery("entity_config_id", configId));
//        }
        String queryString = "user_name:zongzhengjie AND occur_time:>=1571313010603 AND occur_time:<=1571313020603 AND src_geo:(30.5728,104.0668 OR 32.06167,118.77778) AND (_exists_:src_geo AND NOT src_geo:*anet* AND src_geo:*,* AND user_name:zongzhengjie)";
        ElasticsearchConnection connection = new ElasticsearchConnection();
        try {
            connection.connect("172.16.150.60", 9200, "http");
            response = connection.client().search(new SearchRequest("event_201910*")
                    .types("event")
                    .source(new SearchSourceBuilder()
                            .query(QueryBuilders.queryStringQuery(queryString))));
            System.out.println(response.getHits().getHits()[0].getSourceAsMap());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }
}
