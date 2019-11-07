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
import org.elasticsearch.action.index.IndexRequest;
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

    public static final Logger LOG = LoggerFactory.getLogger(Hello.class);

    public static final String ES_HOST = "172.16.150.123";

    public static final Integer ES_PORT = 9200;

    public static final String ES_HTTP_SCHEMA = "http";

    public static final int ITEMS_PER_PAGE = 100;

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
                int totalPages = getTotalPages(officeId);
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
                        sinkToEs(items.getJSONObject(i));
                    }
                    response.close();
                }
            }
        } catch (URISyntaxException | IOException e) {
            LOG.error(e.toString());
        }
        return res;
    }

    private static int getTotalPages(int officeId) {
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

    public static void sinkToEs(JSONObject target) {
        try (ElasticsearchConnection connection = new ElasticsearchConnection()) {
            connection.connect(ES_HOST, ES_PORT, ES_HTTP_SCHEMA);
            connection.client().index(new IndexRequest()
                    .index("yifeng-toy")
                    .type("doctor")
                    .source(JSON.toJSONString(target)));
        } catch (IOException e) {
            LOG.error(e.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        getAllDoctors(getOfficeIds());
    }
}
