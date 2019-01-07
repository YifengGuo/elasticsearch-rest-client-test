package com.yifeng.restclient.data_mockup;

import com.yifeng.restclient.config.ElasticsearchConnection;
import com.yifeng.restclient.mockup.LogonMockupRawLogGenerator;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.util.Date;
import java.util.Map;

import static com.yifeng.restclient.mockup.BaseMockupRawLog.asDate;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

/**
 * Created by guoyifeng on 1/5/19
 */
public class MockRawLogGeneratorTest {
    private ElasticsearchConnection connection;
    private LogonMockupRawLogGenerator generator;

    @Before
    public void initial() {
        try {
            connection = new ElasticsearchConnection();
            connection.connect("172.16.150.189", 29200, "http");
            generator = new LogonMockupRawLogGenerator(connection, new Date());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testA_generateLogonEvent() {
        SearchResponse response = generator.getHistoryDocs("登录事件");
        generator.generateData(response);
    }

    @Test
    public void testB_insertLogonFailureMockupData() {
        generator.insertLogonFailureMockupData();
    }

    @Test
    public void testC_insertLogonCountDeviateBaselineMockupData() {
        generator.insertLogonCountDeviateBaselineMockupData();
    }

    @Test
    public void testD_insertAbnormalLogonLocation() {
        generator.insertAbnormalLogonLocation();
    }

//    1 2   3 4 5 31 30 29        28
//    9 8  7   6  5  4   3  2   1
    @Test
    public void tmp_test() {
        try {
            long count = 0L;
            for (LocalDate date = LocalDate.of( 2018 , Month.NOVEMBER , 1); date.isBefore(LocalDate.of(2018, Month.NOVEMBER, 10)); date = date.plusDays(1)) {
                LogonMockupRawLogGenerator generator = new LogonMockupRawLogGenerator(connection, asDate(date));
                SearchResponse response = generator.getHistoryDocs("");
                BulkRequest bulkRequest = new BulkRequest();
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                for (SearchHit hit : response.getHits().getHits()) {
                    Map<String, Object> currMap = hit.getSourceAsMap();
                    currMap.put("user_name", "quanwu2");
                    if (currMap.containsKey("file_name") && currMap.get("file_name").toString().indexOf("quanwu") > 0) {
                        String value = currMap.get("file_name").toString();
                        currMap.put("file_name", value.replace("quanwu", "quanwu2"));
                    }
                    if (currMap.containsKey("src_filepath") && currMap.get("src_filepath").toString().indexOf("quanwu") > 0) {
                        String value = currMap.get("src_filepath").toString();
                        currMap.put("src_filepath", value.replace("quanwu", "quanwu2"));
                    }
                    if (currMap.containsKey("mail_subject") && currMap.get("mail_subject").toString().indexOf("quanwu") > 0) {
                        String value = currMap.get("mail_subject").toString();
                        currMap.put("mail_subject", value.replace("quanwu", "quanwu2"));
                    }
                    if (currMap.containsKey("user_account") && currMap.get("user_account").toString().indexOf("quanwu") > 0) {
                        String value = currMap.get("user_account").toString();
                        currMap.put("user_account", value.replace("quanwu", "quanwu2"));
                    }
                    if (currMap.containsKey("send_mail") && currMap.get("send_mail").toString().indexOf("quanwu") > 0) {
                        String value = currMap.get("send_mail").toString();
                        currMap.put("send_mail", value.replace("quanwu", "quanwu2"));
                    }
                    long timestamp = (long) currMap.get("occur_time");
                    currMap.put("occur_time", timestamp + 59 * 24 * 60 * 60 * 1000L);
                    bulkRequest.add(new IndexRequest("event_" + simpleDateFormat.format(asDate(date.plusDays(59)))).type("event")
                            .source(currMap)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    count++;
                }
                connection.client().bulk(bulkRequest);
            }
            System.out.println(count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void close() {
        connection.close();
    }
}
