package com.yifeng.restclient.summer_data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yifeng.restclient.config.ElasticsearchConnection;
import com.yifeng.restclient.summer_data.utils.HttpClientFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by guoyifeng on 11/7/19
 */
public class Hello {

    private static final String OFFICE_INDEX = "https://ask.dxy.com/view/i/sectiongroup/list";

    private static final String DOCTORS_BASE_URL = "https://ask.dxy.com/view/i/sectiongroup/member";

    private static final String COMMENT_BASE_URL = "https://ask.dxy.com/view/i/question/comments";

    private static final String HOSPITAL_BASE_URL = "https://dxy.com/health/hospital/";

    private static final String COMMENT_POSTFIX = "comment";

    private static final Logger LOG = LoggerFactory.getLogger(Hello.class);

    private static final String ES_HOST = "172.16.150.123";

    private static final Integer ES_PORT = 9200;

    private static final String ES_HTTP_SCHEMA = "http";

    private static final int ITEMS_PER_PAGE = 100;

    private static final String DEFAULT_INDEX = "yifeng-toy";

    private static final String DOCTOR_TYPE = "doctor";

    private static final String COMMENT_TYPE = "comment";

    private static final String HOSPITAL_TYPE = "hospital";

    private static long index = 109000L;

    public static List<Integer> getOfficeIds() {
        List<Integer> res = new ArrayList<>();
        try (CloseableHttpClient client = HttpClientFactory.createAcceptSelfSignedCertificateClient()) {
            URIBuilder builder = new URIBuilder(OFFICE_INDEX);
            builder.setParameter("page_index", "1")
                    .setParameter("items_per_page", String.valueOf(ITEMS_PER_PAGE))
                    .setParameter("key", "section_list");

            HttpGet request = new HttpGet(builder.build());
            request.setHeader("Content-Type", "application/json; charset=UTF-8");
            CloseableHttpResponse response = client.execute(request);
            JSONObject resJson = JSONObject.parseObject(EntityUtils.toString(response.getEntity(), "utf-8"));
            JSONArray items = resJson.getJSONObject("data").getJSONArray("items");
            for (int i = 0; i < items.size(); ++i) {
                res.add(items.getJSONObject(i).getIntValue("id"));
            }
            response.close();
        } catch (URISyntaxException | IOException e) {
            LOG.error(e.toString());
        }
        return res;
    }

    public static List<JSONObject> getAllDoctors(List<Integer> officeIds) {
        List<JSONObject> res = new ArrayList<>();
        try (CloseableHttpClient client = HttpClientFactory.createAcceptSelfSignedCertificateClient()) {
            for (int officeId : officeIds) {
                int totalPages = getOfficeTotalPages(officeId);
                for (int pageIndex = 1; pageIndex <= totalPages; ++pageIndex) {
                    URIBuilder builder = new URIBuilder(DOCTORS_BASE_URL);
                    builder.setParameter("page_index", String.valueOf(pageIndex))
                            .setParameter("items_per_page", String.valueOf(ITEMS_PER_PAGE))
                            .setParameter("section_group_id", String.valueOf(officeId))
                            .setParameter("ad_status", "1")
                            .setParameter("rank_type", "0")
                            .setParameter("area_type", "0")
                            .setParameter("key", "section_list");

                    HttpGet request = new HttpGet(builder.build());
                    request.setHeader("Content-Type", "application/json; charset=UTF-8");
                    CloseableHttpResponse response = client.execute(request);
                    JSONObject resJson = JSONObject.parseObject(EntityUtils.toString(response.getEntity(), "utf-8"));
                    JSONArray items = resJson.getJSONObject("data").getJSONArray("items");
                    for (int i = 0; i < items.size(); ++i) {
                        sinkTargetToEs(items.getJSONObject(i), DOCTOR_TYPE);
                    }
                    response.close();
                }
            }
        } catch (URISyntaxException | IOException e) {
            LOG.error(e.toString());
        }
        return res;
    }

    private static int getOfficeTotalPages(int officeId) {
        try (CloseableHttpClient client = HttpClientFactory.createAcceptSelfSignedCertificateClient()) {
            URIBuilder builder = new URIBuilder(DOCTORS_BASE_URL);
            builder.setParameter("page_index", String.valueOf(1))
                    .setParameter("items_per_page", String.valueOf(ITEMS_PER_PAGE))
                    .setParameter("section_group_id", String.valueOf(officeId))
                    .setParameter("ad_status", "1")
                    .setParameter("rank_type", "0")
                    .setParameter("area_type", "0")
                    .setParameter("key", "section_list");
            HttpGet request = new HttpGet(builder.build());
            request.setHeader("Content-Type", "application/json; charset=UTF-8");
            CloseableHttpResponse response = client.execute(request);
            JSONObject resJson = JSONObject.parseObject(EntityUtils.toString(response.getEntity(), "utf-8"));
            response.close();
            return resJson.getJSONObject("data").getInteger("total_pages");
        } catch (URISyntaxException | IOException e) {
            LOG.error(e.toString());
        }
        return -1;
    }

    private static int getCommentTotalPages(int doctorId) {
        try (CloseableHttpClient client = HttpClientFactory.createAcceptSelfSignedCertificateClient()) {
            URIBuilder builder = new URIBuilder(COMMENT_BASE_URL);
            builder.setParameter("page_index", String.valueOf(1))
                    .setParameter("items_per_page", String.valueOf(ITEMS_PER_PAGE * 100))
                    .setParameter("doctor_user_id", String.valueOf(doctorId));
            HttpGet request = new HttpGet(builder.build());
            request.setHeader("Content-Type", "application/json; charset=UTF-8");
            CloseableHttpResponse response = client.execute(request);
            JSONObject resJson = JSONObject.parseObject(EntityUtils.toString(response.getEntity(), "utf-8"));
            response.close();
            return resJson.getJSONObject("data").getInteger("total_pages");
        } catch (URISyntaxException | IOException e) {
            LOG.error(e.toString());
        }
        return -1;
    }

    public static void sinkTargetToEs(JSONObject target, String type) {
        try (ElasticsearchConnection connection = new ElasticsearchConnection()) {
            connection.connect(ES_HOST, ES_PORT, ES_HTTP_SCHEMA);
            connection.client().index(new IndexRequest()
                    .index(DEFAULT_INDEX)
                    .type(type)
                    .source(JSON.toJSONString(target)));
        } catch (IOException e) {
            LOG.error(e.toString());
        }
    }

    public static void bulkSinkTargetToEs(JSONArray items, String type) {
        try (ElasticsearchConnection connection = new ElasticsearchConnection()) {
            connection.connect(ES_HOST, ES_PORT, ES_HTTP_SCHEMA);
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < items.size(); ++i) {
                bulkRequest.add(new IndexRequest(DEFAULT_INDEX, type).source(JSON.toJSONString(items.getJSONObject(i)))).timeout(TimeValue.timeValueMinutes(100L));
            }
            connection.client().bulk(bulkRequest);
        } catch (IOException e) {
            LOG.error(e.toString());
        }
    }

    // get comment
    public static List<Integer> getDoctorIds() {
        List<Integer> res = new ArrayList<>();
        try (ElasticsearchConnection connection = new ElasticsearchConnection()) {
            connection.connect(ES_HOST, ES_PORT, ES_HTTP_SCHEMA);
            SearchResponse response = connection.client().search(new SearchRequest("yifeng-toy")
                .types("doctor")
                .source(new SearchSourceBuilder()
                    .size(1000)
                    .query(QueryBuilders.matchAllQuery()))
                    .scroll(new Scroll(TimeValue.timeValueSeconds(10L))));
            while (response.getHits().getHits().length != 0) {
                for (SearchHit hit : response.getHits().getHits()) {
                    res.add((int) hit.getSourceAsMap().get("id"));
                }
                response = connection.client().searchScroll(new SearchScrollRequest(response.getScrollId())
                        .scroll(new Scroll(TimeValue.timeValueSeconds(10L))));
            }
        } catch (IOException e) {
            LOG.error(e.toString());
        }
        return res;
    }

    public static void getAllComments(List<Integer> doctorIds) {
        long count = 0L;
        try (CloseableHttpClient client = HttpClientFactory.createAcceptSelfSignedCertificateClient()) {
            for (int doctorId : doctorIds) {
                int totalPages = getCommentTotalPages(doctorId);
                for (int pageIndex = 1; pageIndex <= totalPages; ++pageIndex) {
                    URIBuilder builder = new URIBuilder(COMMENT_BASE_URL);
                    builder.setParameter("page_index", String.valueOf(pageIndex))
                            .setParameter("items_per_page", String.valueOf(ITEMS_PER_PAGE * 100))
                            .setParameter("doctor_user_id", String.valueOf(doctorId));
                    HttpGet request = new HttpGet(builder.build());
                    request.setHeader("Content-Type", "application/json; charset=UTF-8");
                    CloseableHttpResponse response = client.execute(request);
                    JSONObject resJson = JSONObject.parseObject(EntityUtils.toString(response.getEntity(), "utf-8"));
                    JSONArray items = resJson.getJSONObject("data").getJSONArray("items");
                    bulkSinkTargetToEs(items, COMMENT_TYPE);
                    count += items.size();
                    LOG.info("has written {} comments to es", count);
                    response.close();
                }
            }
        } catch (URISyntaxException | IOException e) {
            LOG.error(e.toString());
        }
    }

    public static void getAllHospitals() {
        JSONArray arr = new JSONArray();
        try {
            while (index < 230000L) {
                Document doc = Jsoup.connect(HOSPITAL_BASE_URL + index)
                        .userAgent("Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0")
                        .referrer("http://www.baidu.com")
                        .ignoreHttpErrors(true)
                        .get();
                if ("出错啦！！！".equals(doc.getElementsByClass("title").text())) continue;
                String grade  = doc.getElementsByClass("grade").text();
                if (grade == null || "".equals(grade)) {
                    grade = "未知等级";
                } else {
                    grade = grade.substring(1, grade.length() - 1);
                }
                String name = doc.getElementsByClass("head").text();
                JSONObject obj = new JSONObject();
                obj.put("id", index);
                obj.put("name", name);
                obj.put("grade", grade);
                arr.add(obj);
                LOG.info("index:{}", index);
                if (++index % 1000 == 0) {
                    LOG.info("has written {} hospitals to es", index);
                    bulkSinkTargetToEs(arr, HOSPITAL_TYPE);
                    arr.clear();
                }
            }
        } catch (IOException e) {
            LOG.error(e.toString());
            index = index / 1000 * 1000;
            getAllHospitals();
        }
    }

    public static void main(String[] args) throws Exception {
//        getAllDoctors(getOfficeIds());
//        getAllComments(getDoctorIds());
        getAllHospitals();

//        Document doc = Jsoup.connect(HOSPITAL_BASE_URL + 6300)
//                .userAgent("Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0")
//                .referrer("http://www.baidu.com")
//                .ignoreHttpErrors(true)
//                .get();
//        System.out.println(doc);
    }
}
