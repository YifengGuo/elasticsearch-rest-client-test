package com.yifeng.restclient.summer_data.utils;

import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

/**
 * Created by guoyifeng on 11/7/19
 */
public class HttpClientFactory {

    public static final Logger LOG = LoggerFactory.getLogger(HttpClientFactory.class);

    public static CloseableHttpClient createAcceptSelfSignedCertificateClient() {
        try {
            // use the TrustSelfSignedStrategy to allow Self Signed Certificates
            SSLContext sslContext = SSLContextBuilder
                    .create()
                    .loadTrustMaterial(new TrustSelfSignedStrategy())
                    .build();

            // we can optionally disable hostname verification.
            // if you don't want to further weaken the security, you don't have to include this.
            HostnameVerifier allowAllHosts = new NoopHostnameVerifier();

            // create an SSL Socket Factory to use the SSLContext with the trust self signed certificate strategy
            // and allow all hosts verifier.
            SSLConnectionSocketFactory connectionFactory = new SSLConnectionSocketFactory(sslContext, allowAllHosts);

            // finally create the HttpClient using HttpClient factory methods and assign the ssl socket factory
            return HttpClients
                    .custom()
                    .setDefaultRequestConfig(RequestConfig.custom()
                            .setCookieSpec(CookieSpecs.STANDARD).build())
                    .setSSLSocketFactory(connectionFactory)
                    .build();
        } catch (Exception e) {
            LOG.error("error in creating closeable http client:", e);
        }
        return null;
    }
}