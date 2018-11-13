package com.yifeng.restclient.test;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
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
import java.util.*;

import static com.yifeng.restclient.config.DatasourceConstant.ES_HOSTS;

public class InsertMockEventDataToES {
    static RestHighLevelClient restClient;
    static RestClient lowLevelClient;
    static final String[] FIELDS = {"﻿standby_number5", "standby_char18", "receive_time", "standby_char19", "event_level", "file_name", "standby_char26", "occur_time", "sa_sp_ap_da_dp", "file_size", "src_geo", "source_host", "event_name", "standby_number6", "src_longitude_latitude_name", "standby_char36", "dev_city", "original_log", "id", "src_city", "src_filepath", "standby_char13", "event_type", "dev_geo", "standby_number7", "first_time", "src_address", "sa_da", "user_account", "file_type", "rule_name", "dev_longitude_latitude_name", "dev_address", "standby_number8", "vendor", "event_id", "user_name", "standby_char27", "product", "mail_subect", "send_mail", "receive_mail", "blind_carbon_copy", "standby_number9"};
    public static void main(String[] args) throws Exception {
        initial();
        String csvFile = "/Users/guoyifeng/Downloads/query_result.csv";
        BufferedReader br = null;
        String line = "";
        String indexName = "event_";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd", Locale.getDefault());
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split(",");

                if (tokens[7].equals("occur_time")) continue; // skip first row

                String timestamp = formatter.format(new Date(Long.parseLong(tokens[7]) * 1000)).replace("/", "");

                // check if index exists
                String curIndexName = indexName + timestamp;
                Response response = lowLevelClient.performRequest("HEAD", curIndexName);
                if (response.getStatusLine().getStatusCode() != 200) {
                    // index does not exist
                    Runtime rt = Runtime.getRuntime();
                    String indexCommand = "curl -X PUT \"172.16.150.149:39200/" + curIndexName + "\"-H 'Content-Type: application/json' -d' { \"settings\" : { \"index\" : { \"number_of_shards\" : 1, \"number_of_replicas\" : 0 } } } '";
                    String settingJson = "{ \"settings\" : { \"index\" : { \"number_of_shards\" : 1, \"number_of_replicas\" : 0 } } }";
                    HttpEntity settingEntity = new NStringEntity(settingJson, ContentType.APPLICATION_JSON);
                    lowLevelClient.performRequest("PUT", "/" + curIndexName, Collections.EMPTY_MAP, settingEntity);

                    // put mappings for the index
//                    String mappingsCommand = "curl -X PUT \"172.16.150.149:39200/" + curIndexName + "\" -H 'Content-Type: application/json' -d' {\"mappings\":{\"event\":{\"properties\":{\"standby_number5\":{\"type\":\"integer\"},\"standby_char18\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"receive_time\":{\"include_in_all\":false,\"format\":\"dateOptionalTime||epoch_millis\",\"type\":\"date\"},\"standby_char19\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_level\":{\"type\":\"integer\"},\"file_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char26\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"occur_time\":{\"include_in_all\":false,\"format\":\"dateOptionalTime||epoch_millis\",\"type\":\"date\"},\"sa_sp_ap_da_dp\":{\"type\":\"string\"},\"file_size\":{\"type\":\"long\"},\"src_geo\":{\"type\":\"geo_point\"},\"source_host\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number6\":{\"type\":\"integer\"},\"src_longitude_latitude_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char36\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_city\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"original_log\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"id\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"src_city\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"src_filepath\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char13\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_type\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_geo\":{\"type\":\"geo_point\"},\"standby_number7\":{\"type\":\"integer\"},\"first_time\":{\"include_in_all\":false,\"format\":\"dateOptionalTime||epoch_millis\",\"type\":\"date\"},\"src_address\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"sa_da\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"user_account\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"file_type\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"rule_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_longitude_latitude_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_address\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number8\":{\"type\":\"integer\"},\"vendor\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_id\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"user_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char27\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"product\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"mail_subect\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"send_mail\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"receive_mail\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"blind_carbon_copy\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number9\":{\"type\":\"integer\"}}}}}' ";
                    String mappingJson = "{ \"properties\":{\"standby_number5\":{\"type\":\"integer\"},\"standby_char18\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"receive_time\":{\"type\":\"long\"},\"standby_char19\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_level\":{\"type\":\"integer\"},\"file_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char26\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"occur_time\":{\"include_in_all\":false,\"format\":\"dateOptionalTime||epoch_millis\",\"type\":\"date\"},\"sa_sp_ap_da_dp\":{\"type\":\"string\"},\"file_size\":{\"type\":\"long\"},\"src_geo\":{\"type\":\"string\", \"index\":\"not_analyzed\"},\"source_host\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number6\":{\"type\":\"integer\"},\"src_longitude_latitude_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char36\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_city\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"original_log\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"id\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"src_city\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"src_filepath\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char13\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_type\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_geo\":{\"type\":\"geo_point\"},\"standby_number7\":{\"type\":\"integer\"},\"first_time\":{\"type\":\"string\"},\"src_address\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"sa_da\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"user_account\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"file_type\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"rule_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_longitude_latitude_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_address\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number8\":{\"type\":\"integer\"},\"vendor\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_id\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"user_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char27\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"product\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"mail_subect\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"send_mail\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"receive_mail\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"blind_carbon_copy\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number9\":{\"type\":\"string\", \"index\" : \"not_analyzed\"}}}}";
                    HttpEntity mappingEntity = new NStringEntity(mappingJson, ContentType.APPLICATION_JSON);
                    lowLevelClient.performRequest("PUT", "/" + curIndexName + "/_mapping/event", Collections.EMPTY_MAP, mappingEntity);

                    // index document to es
                    Map<String, Object> curMap = doc(tokens);
                    IndexResponse indexResponse = restClient.index(new IndexRequest(curIndexName, "event").source(curMap));

                } else {
                    // index exists
                    // index document directly
                    Map<String, Object> curMap = doc(tokens);
                    IndexResponse indexResponse = restClient.index(new IndexRequest(curIndexName, "event").source(curMap));
                }
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

    private static void initial() {
        lowLevelClient = RestClient.builder(
                new HttpHost(ES_HOSTS, 39200, "http")).build();
        restClient = new RestHighLevelClient(lowLevelClient);
    }

    private static Map<String, Object> doc(String[] tokens) {
        Map<String, Object> res = new HashMap<>();
        for (int i = 0; i < tokens.length; i++) {
            if (i == 7) {
                res.put(FIELDS[i], Long.parseLong(tokens[i]) * 1000L);
                continue;
            }
            res.put(FIELDS[i], tokens[i]);
        }
        return res;
    }
}
