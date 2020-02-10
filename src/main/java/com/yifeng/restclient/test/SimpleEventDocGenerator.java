package com.yifeng.restclient.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yifeng.restclient.config.ElasticsearchConnection;
import com.yifeng.restclient.pojo.UEBAUser;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.yifeng.restclient.pojo.UEBAUser.createDefault;

/**
 * Created by guoyifeng on 9/23/19
 */
public class SimpleEventDocGenerator {
    private static final String INDEX_PREFIX = "event_";
    private static final String TYPE = "event";
    private static final long DAY = 86400_000L;

    private static final String ES_HOSTS = "172.16.150.123";
    private static final int PORTS = 9200;
    private static final String HTTP_SCHEMA = "http";


    static RestHighLevelClient restClient;
    static RestClient lowLevelClient;
    static final String[] FIELDS = {"user_name", "file_name", "file_size", "event_id", "occur_time", "file_type"};
    static final String[] OLD_USERS = {"user_0", "user_1", "user_2", "user_3", "user_4", "user_5"};
    static final String[] BLACKLIST_USERS = {"blacklist_user_1", "blacklist_user_2"};
    static final String[] FILE_POSTFIX = {".doc", ".py", ".exe"};
    static final String[] DEV_IP = {"172.16.150.60", "172.16.150.189", "172.16.150.123", "172.16.150.172"};
    static final String[] SRC_IP = {"172.16.150.106", "172.16.150.107", "172.16.150.108", "172.16.150.109"};
    static final String[] FILE_EVENT_NAMES = {"文件访问", "云桌面文件访问"};
    static final String[] FILE_EVENT_TYPE = {"拷贝", "删除", "剪切", "上传", ""};
    static final String[] LOGON_EVENT_TYPE = {"云登录", "手机登录", "邮箱登录", "客户端登录", ""};
    static final String[] FILE_TYPE = {".doc", ".py", ".exe", ""};
    static final String[] FILE_PATH = {"/root", "/opt", "/opt/hansight", "~/Downloads", ""};
    static final String[] DOMAIN = {"baidu cloud", "alicloud", "aws", ""};
    static final String[] CLOUD_PATH = {"/root", "/opt", "/opt/hansight", "~/Downloads", ""};
    static final String[] DEPARTMENT = {"qa", "dev", "hr", "admin", "sale", ""};
    static final String[] LOGON_EVENT_NAMES = {"4A", "统一身份认证", "logon", ""};
    static final String[] MAC_ADDRESS = {"88:e9:fe:87:d5:0f", "88:e9:fe:87:d5:03", "88:e9:fe:87:d5:01", ""};
    static final String[] UID_ARR = {"111", "123"};
    static final String[] USER_DEPT = {"dept_0", "dept_1", "dept_2", "dept_3", "dept_4", "dept_5"};


    // taiping
    static final String[] orgIds = {"10000", "20000", "30000", "40000", "50000", "60000"};
    static final String[] orgLevel2Names = {"成都分公司", "北京分公司", "南京分公司", "上海分公司", "深圳分公司", "广州分公司"};
    static final String[] orgfullnames = {"四川/成都分公司", "北京/北京分公司", "江苏/南京分公司", "上海/上海分公司", "广东/深圳分公司", "广东/广州分公司"};
    static final String[] departmentIds = {"1", "2", "3", "4", "5", "6"};
    static final String[] uids = {"111", "222", "333", "444", "555", "666"};


    static SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd", Locale.getDefault());
    public static void main(String[] args) throws Exception {
        JSONArray arr = new JSONArray();
        for (int i = 0; i < 1200; ++i) {
//            arr.add(initDoc(new Long[]{System.currentTimeMillis() - 90 * DAY, System.currentTimeMillis()}));
            arr.add(initDoc(new Long[]{1573016211156L}));
        }
        indexDoc(arr);
//        initTestUsers();

//        for (int i = 0; i < 10; ++i) {
//            indexDoc(initBlacklistDoc(1569513600000L));
//        }


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

    public static JSONObject initDoc(Long[] period) {
        JSONObject obj = new JSONObject();
        Long now;
        if (period.length == 1) {
             now = period[0];
        } else {
             long gt = period[0];
             long lt = period[1];
             now = ThreadLocalRandom.current().nextLong(gt, lt);
        }

        obj.put("src_city", "Intranet");
        obj.put("dev_city", "Intranet");
        int userIndex = 3;
//        int userIndex = getRandomIndex(OLD_USERS.length);
        obj.put("user_name", OLD_USERS[userIndex]);
        String postfix = FILE_POSTFIX[getRandomIndex(FILE_POSTFIX.length)];
        obj.put("file_name", getAlphaNumericString(6) + postfix);
        obj.put("receive_time", now);
        obj.put("dev_address", DEV_IP[getRandomIndex(DEV_IP.length)]);
        obj.put("file_size", ThreadLocalRandom.current().nextInt(2000));
        obj.put("occur_time", now);
        obj.put("file_type", postfix);
        obj.put("src_address", SRC_IP[getRandomIndex(SRC_IP.length)]);
        obj.put("event_name", FILE_EVENT_NAMES[getRandomIndex(FILE_EVENT_NAMES.length)]);
        obj.put("event_type", FILE_EVENT_TYPE[getRandomIndex(FILE_EVENT_TYPE.length)]);
        obj.put("file_path", FILE_PATH[getRandomIndex(FILE_PATH.length)]);
        obj.put("domain", DOMAIN[getRandomIndex(DOMAIN.length)]);
        obj.put("cloud_path", CLOUD_PATH[getRandomIndex(DOMAIN.length)]);
        obj.put("dst_address", SRC_IP[getRandomIndex(SRC_IP.length)]);
        obj.put("department", USER_DEPT[userIndex]);
        obj.put("org_id", orgIds[userIndex]);
        obj.put("orgfullname", orgfullnames[userIndex]);
        obj.put("department_id", departmentIds[userIndex]);
        obj.put("org_name_level2", orgLevel2Names[userIndex]);
        obj.put("uid", uids[userIndex]);
        return obj;
    }

    public static JSONObject initLogonDoc(Long[] period) {
        JSONObject obj = new JSONObject();
        Long now;
        if (period.length == 1) {
            now = period[0];
        } else {
            long gt = period[0];
            long lt = period[1];
            now = ThreadLocalRandom.current().nextLong(gt, lt);
        }
        obj.put("uid", UID_ARR[getRandomIndex(UID_ARR.length)]);
//        obj.put("user_name", OLD_USERS[getRandomIndex(OLD_USERS.length)]);
        obj.put("event_name", LOGON_EVENT_NAMES[getRandomIndex(LOGON_EVENT_NAMES.length)]);
        obj.put("occur_time", now);
        obj.put("dst_address", SRC_IP[getRandomIndex(SRC_IP.length)]);
        obj.put("src_address", SRC_IP[getRandomIndex(SRC_IP.length)]);
        obj.put("result", ThreadLocalRandom.current().nextBoolean());
        obj.put("department", DEPARTMENT[getRandomIndex(DEPARTMENT.length)]);
        obj.put("mac_address", MAC_ADDRESS[getRandomIndex(MAC_ADDRESS.length)]);
        obj.put("event_type", LOGON_EVENT_TYPE[getRandomIndex(LOGON_EVENT_TYPE.length)]);
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

    public static void indexDoc(JSONArray arr) {
        try (ElasticsearchConnection connection = new ElasticsearchConnection()) {
            connection.connect(ES_HOSTS, PORTS, HTTP_SCHEMA);
            BulkRequest bulkRequest = new BulkRequest();
            int cnt = 0;
            for (int i = 0; i < arr.size(); ++i) {
                JSONObject curr = arr.getJSONObject(i);
                long now = curr.getLong("occur_time");
                String datePostfix = formatter.format(now).replace("/", "");
                String indexName = INDEX_PREFIX + datePostfix;
                bulkRequest.add(new IndexRequest().index(indexName).type(TYPE).source(curr.toJSONString()));
                if (++cnt % 2000 == 0) {
                    connection.client().bulk(bulkRequest);
                    bulkRequest = new BulkRequest();
                }
            }
            if (bulkRequest.numberOfActions() > 0) {
                connection.client().bulk(bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
                bulkRequest = null;
            }
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

    static void initTestUsers() throws Exception {
        String uuid = UUID.randomUUID().toString();
        // init user_config
        ElasticsearchConnection connection = new ElasticsearchConnection();
        connection.connect(ES_HOSTS, PORTS, HTTP_SCHEMA);
        for (int i = 0; i < OLD_USERS.length; ++i) {
            String username = OLD_USERS[i];
            UEBAUser user = createDefault();
            user.setId(username);
            user.setUserConfigId(uuid);
            user.setName(username);
            int index = i;
            user.setOrgFullName(orgfullnames[index]);
            user.setOrgNameLevel2(orgLevel2Names[index]);
            user.setOrgId(orgIds[index]);
            user.setPhone("--");
            user.setDepartment(USER_DEPT[index]);
            user.setDepartmentId(departmentIds[index]);
            user.setUid(uids[index]);
            connection.client().index(new IndexRequest().index("ueba_settings").type("user_info").source(user.toString()));
        }
        connection.close();
    }

    static Map<String, Integer> connectUserAndIndex() {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < OLD_USERS.length; ++i) {
            map.put(OLD_USERS[i], i);
        }
        return map;
    }

}
