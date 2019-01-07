package com.yifeng.restclient.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Justin Wan justinxcwan@gmail.com
 */
public class ElasticsearchConnection implements Closeable {
    protected SimpleDateFormat indicesPattern;
    protected String indices;
    // Most likely to be daily or weekly
    protected long indexInterval;
    protected String[] types = {"*"};
    protected RestHighLevelClient client;
    protected RestClient lowLevelClient;

    private String connectedHosts; // currently do not consider connecting to multiple hosts cases
    private int connectedPort;
    private String connectedHttpSchema;

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchConnection.class);

    public ElasticsearchConnection setIndices(String indices) {
        this.indices = indices;
        // clear any preset indices pattern
        this.indicesPattern = null;

        return this;
    }

    public ElasticsearchConnection setIndicesPattern(String pattern) {
        this.indices = null;

        // Try to be compatible with Kibana settings
        // [logstash-]YYYY.MM.DD.HH
        // [logstash-]YYYY.MM.DD
        // [logstash-]GGGG.WW
        // [logstash-]YYYY.MM
        // [logstash-]YYYY
        Pattern p = Pattern.compile("(.*)\\[(.*?)\\](.*)");
        Matcher m = p.matcher(pattern);
        m.matches();
        String datepattern = m.group(2).replace("YYYY", "yyyy")
                .replace("DD", "dd")
                .replace("WW", "ww")
                .replace("GGGG", "yyyy");

        pattern = m.group(1) + "'" + datepattern + "'" + m.group(3);

        this.indicesPattern = new SimpleDateFormat(pattern);

        if (pattern.indexOf("HH") >= 0) {
            indexInterval = 60 * 60 * 1000L; // one hour
        } else if (pattern.indexOf("dd") >= 0) {
            indexInterval = 24 * 60 * 60 * 1000L; // one day
        } else if (pattern.indexOf("ww") >= 0) {
            indexInterval = 7 * 24 * 60 * 60 * 1000L; // one week
        } else if (pattern.indexOf("MM") >= 0) {
            // TODO()  possible bug?
            indexInterval = 30 * 7 * 24 * 60 * 60 * 1000L; // one month
        } else if (pattern.indexOf("yyyy") >= 0) {
            indexInterval = 365 * 24 * 60 * 60 * 1000L; // one year
        }
        return this;
    }

    public RestHighLevelClient client() {
        return client;
    }

    public RestClient getLowLevelClient() {
        return lowLevelClient;
    }

    public String getTypes() {
        return Strings.arrayToCommaDelimitedString(types);
    }

    public ElasticsearchConnection setTypes(String... types) {
        this.types = types;

        return this;
    }

    public ElasticsearchConnection connect(String hosts, int port, String httpSchema) throws UnknownHostException {
        lowLevelClient = RestClient.builder(new HttpHost(hosts, port, httpSchema))
                .setRequestConfigCallback(
                        requestConfigBuilder -> requestConfigBuilder
                                // connect timeout while es gc ?
                                .setConnectTimeout(30_000)
                                .setSocketTimeout(120_000))
                .setMaxRetryTimeoutMillis(60_000).build();

        client = new RestHighLevelClient(lowLevelClient);
        // cluster name is set in elasticsearch.yml
        // unnecessary to set it again
        connectedHosts = hosts;
        connectedPort = port;
        connectedHttpSchema = httpSchema;

        return this;
    }

    public static ElasticsearchConnection of(String hosts, int port, String httpSchema) {
        ElasticsearchConnection connection = new ElasticsearchConnection();
        try {
            connection.connect(hosts, port, httpSchema);
        } catch (UnknownHostException e) {
            LOG.error("Error occurred in initialize es connection. host: {}, port: {}, schema: {}.", hosts, port, httpSchema);
        }
        return connection;
    }

    /**
     * Find indices matching range [gte, lt).
     *
     * @param gte timestamp from
     * @param lt  timestamp until
     **/
    public String[] indicesForRange(long gte, long lt) {
        if (indices != null) {
            // No timestamp in indices
            return new String[]{indices};
        } else if (indicesPattern != null) {
            ArrayList<String> list = new ArrayList<>();

            // Use indexInterval/2 instead of indexInterval to avoid bugs when
            // intervals are month or year
            for (long ts = gte; ts <= lt; ts += indexInterval / 2) {
                String index = this.indicesPattern.format(new Date(ts));
                if (list.size() == 0 || !index.equals(list.get(list.size() - 1))) {
                    list.add(index);
                }
            }

            return list.toArray(new String[]{});
        } else {
            throw new IllegalArgumentException("indices and indicesPattern are all null");
        }
    }

    public void close() {
        if (client != null) {
            try {
                lowLevelClient.close();
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    public String getConnectedHosts() {
        return connectedHosts;
    }

    public int getConnectedPort() {
        return connectedPort;
    }

    public String getHttpSchema() {
        return connectedHttpSchema;
    }
}
