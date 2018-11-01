package com.yifeng.restclient.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import static com.yifeng.restclient.config.DatasourceConstant.ES_HOSTS;

public class Test {
    static RestHighLevelClient restClient;
    public static void main(String[] args) throws Exception {
        initial();
        String csvFile = "/Users/guoyifeng/Downloads/query_result.csv";
        BufferedReader br = null;
        String line = "";
        String indexName = "event_";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd", Locale.getDefault());
        try {
            br = new BufferedReader(new FileReader(csvFile));
            int count = 2;
            while ((line = br.readLine()) != null  && count > 0) {
                String[] tokens = line.split(",");
                if (tokens[7].equals("occur_time")) continue; // skip first row

                String timestamp = formatter.format(new Date(Long.parseLong(tokens[7]) * 1000)).replace("/", "");
                System.out.println(timestamp);
                // insert document to corresponding indices

                Response response = restClient.getLowLevelClient().performRequest("HEAD", "/" + indexName + timestamp);
                int statusCode = response.getStatusLine().getStatusCode();
                System.out.println(statusCode);
                String curIndexName = indexName + timestamp;
                if (statusCode == 404) {
                    // index does not exist
                    CreateIndexRequest request = new CreateIndexRequest(curIndexName);
                    request.settings(Settings.builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                    );
                    CreateIndexResponse createIndexResponse = restClient.indices().create(request, RequestOptions.DEFAULT);

                    PutMappingRequest mappingRequest = new PutMappingRequest(curIndexName);
                    mappingRequest.type("event");
                    mappingRequest.source("message", "type=text");

                } else {
                    // index exists


                }

                count--;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void initial() throws Exception {
        restClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ES_HOSTS, 29200, "http")));
    }
}
