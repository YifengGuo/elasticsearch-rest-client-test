package com.yifeng.restclient.test;

import com.alibaba.fastjson.JSONObject;
import com.yifeng.restclient.config.ElasticsearchConnection;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by guoyifeng on 9/23/19
 */
public class SimpleEventDocGenerator {
    private static final String INDEX_PREFIX = "event_";
    private static final String TYPE = "event";

    private static final String ES_HOSTS = "172.16.150.60";
    private static final int PORTS = 9200;
    private static final String HTTP_SCHEMA = "http";


    static RestHighLevelClient restClient;
    static RestClient lowLevelClient;
    static final String[] FIELDS = {"user_name", "file_name", "file_size", "event_id", "occur_time", "file_type"};
    static final String[] OLD_USERS = {"YIFENG", "new_user_1"};
    static final String[] BLACKLIST_USERS = {"blacklist_user_1", "blacklist_user_2"};
    static final String[] FILE_POSTFIX = {".doc", ".py", ".exe"};
    static final String[] DEV_IP = {"172.16.150.60", "172.16.150.189", "172.16.150.123", "172.16.150.172"};
    static final String[] SRC_IP = {"172.16.150.106", "172.16.150.107", "172.16.150.108", "172.16.150.109"};
    static SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd", Locale.getDefault());
    public static void main(String[] args) throws Exception {
//        for (int i = 0; i < 2; ++i) {
//            indexDoc(initDoc(1569292140000L));
//        }

        for (int i = 0; i < 10; ++i) {
            indexDoc(initBlacklistDoc(1569513600000L));
        }


//        for (int i = 0; i < 5; i++) {
//            indexDoc(initFlagDoc(1569315390000L));
//        }
    }

    public static JSONObject initBlacklistDoc(long time) {
        JSONObject obj = new JSONObject();
        Long now;
        if (time == 0L) {
            now = System.currentTimeMillis();
        } else {
            now = time;
        }

        obj.put("src_city", "Intranet");
        obj.put("dev_city", "Intranet");
        obj.put("user_name", BLACKLIST_USERS[getRandomIndex(OLD_USERS.length)]);
        String postfix = FILE_POSTFIX[getRandomIndex(FILE_POSTFIX.length)];
        obj.put("file_name", getAlphaNumericString(6) + postfix);
        obj.put("receive_time", now);
        obj.put("dev_address", DEV_IP[getRandomIndex(DEV_IP.length)]);
        obj.put("file_size", ThreadLocalRandom.current().nextInt(2000));
        obj.put("occur_time", now);
        obj.put("file_type", postfix);
        obj.put("src_address", SRC_IP[getRandomIndex(SRC_IP.length)]);
        return obj;
    }

    public static JSONObject initDoc(long time) {
        JSONObject obj = new JSONObject();
        Long now;
        if (time == 0L) {
             now = System.currentTimeMillis();
        } else {
             now = time;
        }

        obj.put("src_city", "Intranet");
        obj.put("dev_city", "Intranet");
        obj.put("user_name", OLD_USERS[getRandomIndex(OLD_USERS.length)]);
        String postfix = FILE_POSTFIX[getRandomIndex(FILE_POSTFIX.length)];
        obj.put("file_name", getAlphaNumericString(6) + postfix);
        obj.put("receive_time", now);
        obj.put("dev_address", DEV_IP[getRandomIndex(DEV_IP.length)]);
        obj.put("file_size", ThreadLocalRandom.current().nextInt(2000));
        obj.put("occur_time", now);
        obj.put("file_type", postfix);
        obj.put("src_address", SRC_IP[getRandomIndex(SRC_IP.length)]);
        return obj;
    }

    public static JSONObject initFlagDoc(long time) {
        JSONObject obj = new JSONObject();
        Long now;
        if (time == 0L) {
            now = System.currentTimeMillis();
        } else {
            now = time;
        }

        obj.put("src_city", "Intranet");
        obj.put("dev_city", "Intranet");
        obj.put("user_name", "flag_user");
        String postfix = FILE_POSTFIX[getRandomIndex(FILE_POSTFIX.length)];
        obj.put("file_name", getAlphaNumericString(6) + postfix);
        obj.put("receive_time", now);
        obj.put("dev_address", DEV_IP[getRandomIndex(DEV_IP.length)]);
        obj.put("file_size", ThreadLocalRandom.current().nextInt(2000));
        obj.put("occur_time", now);
        obj.put("file_type", postfix);
        obj.put("src_address", SRC_IP[getRandomIndex(SRC_IP.length)]);
        return obj;
    }

    public static void indexDoc(JSONObject obj) {
        try (ElasticsearchConnection connection = new ElasticsearchConnection()) {
            connection.connect(ES_HOSTS, PORTS, HTTP_SCHEMA);
            long now = System.currentTimeMillis();
            String datePostfix = formatter.format(now).replace("/", "");
            String indexName = INDEX_PREFIX + datePostfix;
            IndexResponse response = connection.client().index(new IndexRequest()
                    .index(indexName)
                    .type(TYPE)
                    .source(obj.toJSONString())
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static String getAlphaNumericString(int n) {

        // chose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index
                    = (int)(AlphaNumericString.length()
                    * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                    .charAt(index));
        }

        return sb.toString();
    }

    static int getRandomIndex(int n) {
        return ThreadLocalRandom.current().nextInt(n);
    }

    static void deleteFlagUserEventAndUserInfo() {
        try (ElasticsearchConnection connection = new ElasticsearchConnection()) {
            connection.connect(ES_HOSTS, PORTS, HTTP_SCHEMA);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
