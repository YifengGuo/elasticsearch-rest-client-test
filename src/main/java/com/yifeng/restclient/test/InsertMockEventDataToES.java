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

    private static final String INDEX = "testevent_";
    private static final String TYPE = "event";

    private static final String ES_HOSTS = "172.16.106.178";
    private static final int PORTS = 9200;
    private static final String HTTP_SCHEMA = "http";


    static RestHighLevelClient restClient;
    static RestClient lowLevelClient;
    static final String[] FIELDS = {"﻿standby_number5","standby_char18","receive_time","standby_char19","event_level","file_name","standby_char26","occur_time","sa_sp_ap_da_dp","file_size","source_host","event_name","standby_number6","src_longitude_latitude_name","standby_char36","dev_city","original_log","id","src_city","src_filepath","standby_char13","event_type","standby_number7","first_time","src_address","sa_da","user_account","file_type","rule_name","dev_longitude_latitude_name","dev_address","standby_number8","vendor","event_id","user_name","standby_char27","product","mail_subject","send_mail","receive_mail","blind_carbon_copy","standby_number9","result"};
    public static void main(String[] args) throws Exception {
        initial();
        String csvFile = "/Users/guoyifeng/Downloads/query_result.csv";
        BufferedReader br = null;
        String line = "";
        String indexName = INDEX;
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
                    String settingJson = "{ \"settings\" : { \"index\" : { \"number_of_shards\" : 1, \"number_of_replicas\" : 0 } } }";
                    HttpEntity settingEntity = new NStringEntity(settingJson, ContentType.APPLICATION_JSON);
                    lowLevelClient.performRequest("PUT", "/" + curIndexName, Collections.EMPTY_MAP, settingEntity);

                    // put mappings for the index
//                    String mappingsCommand = "curl -X PUT \"172.16.150.149:39200/" + curIndexName + "\" -H 'Content-Type: application/json' -d' {\"mappings\":{\"event\":{\"properties\":{\"standby_number5\":{\"type\":\"integer\"},\"standby_char18\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"receive_time\":{\"include_in_all\":false,\"format\":\"dateOptionalTime||epoch_millis\",\"type\":\"date\"},\"standby_char19\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_level\":{\"type\":\"integer\"},\"file_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char26\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"occur_time\":{\"include_in_all\":false,\"format\":\"dateOptionalTime||epoch_millis\",\"type\":\"date\"},\"sa_sp_ap_da_dp\":{\"type\":\"string\"},\"file_size\":{\"type\":\"long\"},\"src_geo\":{\"type\":\"geo_point\"},\"source_host\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number6\":{\"type\":\"integer\"},\"src_longitude_latitude_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char36\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_city\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"original_log\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"id\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"src_city\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"src_filepath\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char13\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_type\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_geo\":{\"type\":\"geo_point\"},\"standby_number7\":{\"type\":\"integer\"},\"first_time\":{\"include_in_all\":false,\"format\":\"dateOptionalTime||epoch_millis\",\"type\":\"date\"},\"src_address\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"sa_da\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"user_account\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"file_type\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"rule_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_longitude_latitude_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_address\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number8\":{\"type\":\"integer\"},\"vendor\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_id\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"user_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char27\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"product\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"mail_subect\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"send_mail\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"receive_mail\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"blind_carbon_copy\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number9\":{\"type\":\"integer\"}}}}}' ";
                    // for 149
//                    String mappingJson = "{ \"properties\":{\"standby_number5\":{\"type\":\"integer\"},\"standby_char18\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"receive_time\":{\"type\":\"long\"},\"standby_char19\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_level\":{\"type\":\"integer\"},\"file_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char26\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"occur_time\":{\"include_in_all\":false,\"format\":\"dateOptionalTime||epoch_millis\",\"type\":\"date\"},\"sa_sp_ap_da_dp\":{\"type\":\"string\"},\"file_size\":{\"type\":\"long\"},\"src_geo\":{\"type\":\"string\", \"index\":\"not_analyzed\"},\"source_host\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number6\":{\"type\":\"integer\"},\"src_longitude_latitude_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char36\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_city\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"original_log\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"id\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"src_city\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"src_filepath\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char13\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_type\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_geo\":{\"type\":\"geo_point\"},\"standby_number7\":{\"type\":\"integer\"},\"first_time\":{\"type\":\"string\"},\"src_address\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"sa_da\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"user_account\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"file_type\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"rule_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_longitude_latitude_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"dev_address\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number8\":{\"type\":\"integer\"},\"vendor\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"event_id\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"user_name\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_char27\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"product\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"mail_subect\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"send_mail\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"receive_mail\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"blind_carbon_copy\":{\"type\":\"string\",\"index\":\"not_analyzed\"},\"standby_number9\":{\"type\":\"string\", \"index\" : \"not_analyzed\"}}}}";

                    // for 60
                    String mappingJson = "{ \"properties\":{\"standby_number5\":{\"type\":\"integer\"},\"standby_char18\":{\"type\":\"keyword\"},\"receive_time\":{\"type\":\"long\"},\"standby_char19\":{\"type\":\"keyword\"},\"event_level\":{\"type\":\"integer\"},\"file_name\":{\"type\":\"keyword\"},\"standby_char26\":{\"type\":\"keyword\"},\"occur_time\":{\"format\":\"dateOptionalTime||epoch_millis\",\"type\":\"date\"},\"sa_sp_ap_da_dp\":{\"type\":\"keyword\"},\"file_size\":{\"type\":\"long\"},\"src_geo\":{\"type\":\"keyword\", \"index\":\"false\"},\"source_host\":{\"type\":\"keyword\"},\"event_name\":{\"type\":\"keyword\"},\"standby_number6\":{\"type\":\"integer\"},\"src_longitude_latitude_name\":{\"type\":\"keyword\"},\"standby_char36\":{\"type\":\"keyword\"},\"dev_city\":{\"type\":\"keyword\"},\"original_log\":{\"type\":\"keyword\"},\"id\":{\"type\":\"keyword\"},\"src_city\":{\"type\":\"keyword\"},\"src_filepath\":{\"type\":\"keyword\"},\"standby_char13\":{\"type\":\"keyword\"},\"event_type\":{\"type\":\"keyword\"},\"dev_geo\":{\"type\":\"geo_point\"},\"standby_number7\":{\"type\":\"integer\"},\"first_time\":{\"type\":\"keyword\"},\"src_address\":{\"type\":\"keyword\"},\"sa_da\":{\"type\":\"keyword\"},\"user_account\":{\"type\":\"keyword\"},\"file_type\":{\"type\":\"keyword\"},\"rule_name\":{\"type\":\"keyword\"},\"dev_longitude_latitude_name\":{\"type\":\"keyword\"},\"dev_address\":{\"type\":\"keyword\"},\"standby_number8\":{\"type\":\"integer\"},\"vendor\":{\"type\":\"keyword\"},\"event_id\":{\"type\":\"keyword\"},\"user_name\":{\"type\":\"keyword\"},\"standby_char27\":{\"type\":\"keyword\"},\"product\":{\"type\":\"keyword\"},\"mail_subject\":{\"type\":\"keyword\"},\"send_mail\":{\"type\":\"keyword\"},\"receive_mail\":{\"type\":\"keyword\"},\"blind_carbon_copy\":{\"type\":\"keyword\"},\"standby_number9\":{\"type\":\"keyword\", \"index\" : \"false\"}, \"result\":{\"type\":\"keyword\"}}}}";
                    HttpEntity mappingEntity = new NStringEntity(mappingJson, ContentType.APPLICATION_JSON);
                    lowLevelClient.performRequest("PUT", "/" + curIndexName + "/_mapping/" + TYPE, Collections.EMPTY_MAP, mappingEntity);

                    // index document to es
                    Map<String, Object> curMap = doc(tokens);
                    IndexResponse indexResponse = restClient.index(new IndexRequest(curIndexName, TYPE).source(curMap));

                } else {
                    // index exists
                    // index document directly
                    Map<String, Object> curMap = doc(tokens);
                    IndexResponse indexResponse = restClient.index(new IndexRequest(curIndexName, TYPE).source(curMap));
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
                new HttpHost(ES_HOSTS, PORTS, HTTP_SCHEMA)).build();
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
