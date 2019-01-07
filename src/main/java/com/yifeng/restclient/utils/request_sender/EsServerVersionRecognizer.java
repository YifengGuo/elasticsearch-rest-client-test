package com.yifeng.restclient.utils.request_sender;

/**
 * Created by guoyifeng on 10/24/18
 */

import com.alibaba.fastjson.JSONObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * To get the major version of current elasticsearch server
 */
public class EsServerVersionRecognizer {
    private static final Logger LOG = LoggerFactory.getLogger(EsServerVersionRecognizer.class);
    public static int getEsServerMajorVersion(String schema, String hosts, int port) {
        try {
            CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build();
            HttpGet getRequest = new HttpGet(schema + File.pathSeparator + '/' + '/' + hosts + File.pathSeparator + port);
            CloseableHttpResponse response = closeableHttpClient.execute(getRequest);
            String responseJSON = EntityUtils.toString(response.getEntity(), "utf-8");
            JSONObject obj = JSONObject.parseObject(responseJSON);
            JSONObject version = obj.getJSONObject("version");
            String versionNumber = version.getString("number");
            return Integer.parseInt(versionNumber.split("\\.")[0]);
        } catch (IOException e) {
            LOG.error("cannot get the version number of es server");
        }
        LOG.error("cannot get the correct version number of es server");
        return -1;
    }
}
