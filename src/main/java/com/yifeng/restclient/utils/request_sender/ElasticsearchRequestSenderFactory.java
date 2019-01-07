package com.yifeng.restclient.utils.request_sender;

import com.yifeng.restclient.config.ElasticsearchConnection;

/**
 * Created by guoyifeng on 12/20/18
 */
public class ElasticsearchRequestSenderFactory {

    public ElasticsearchRequestSenderFactory() {
    }

    public static ElasticsearchRequestSender createSender(ElasticsearchConnection connection) {
        int esVersion = EsServerVersionRecognizer.getEsServerMajorVersion(connection.getHttpSchema(), connection.getConnectedHosts(), connection.getConnectedPort());
        return esVersion <= 2 ? new ElasticsearchLowLevelRequestSender(connection) : new ElasticsearchHighLevelRequestSender(connection);
    }

    public static ElasticsearchRequestSender createSender(int esMajorVersion, ElasticsearchConnection connection) {
        return esMajorVersion <= 2 ? new ElasticsearchLowLevelRequestSender(connection) : new ElasticsearchHighLevelRequestSender(connection);
    }
}
