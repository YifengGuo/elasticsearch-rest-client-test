package com.yifeng.restclient.pojo;

/**
 * Created by liujia on 2018/11/21.
 */
public class UEBA {
    public static final String REFRESH_USER_TOPIC = "refresh-users";

    public static final String[] DEFAULT_GROUP_FIELDS = {"role", "department"};
    public static final String DEFAULT_TIME_FIELD = "@timestamp";
    public static final String DEFAULT_USER_FIELD = "userId";

    public static final String UEBA_SETTINGS = "ueba_settings";
    public static final String UEBA_ALARM_INDEX = "ueba_alarm";
    public static final String UEBA_ALARM_PREVIEW_INDEX = "ueba_alarm_preview";
    public static final String ENT_ASSET_INDEX = "asset_confirmed";
    public static final String ENT_ASSET_TYPE = "asset";
    public static final String UEBA_ALARM_TYPE_INCIDENT = "anomaly_incidents";
    public static final String UEBA_ALARM_TYPE_ALERT = "anomaly_alerts";
    public static final String UEBA_USER_TYPE = "user_info";
    public static final String UEBA_SCENARIO_SETTING_TYPE = "scenario";
    public static final String MLA_TYPE = "mla";
    public static final String ENTITY_CONFIG_TYPE = "entity_config";
    public static final String DATASOURCE_SETTING_TYPE = "datasource";
    public static final String MLA_MODEL_TYPE = "model";

    public static final String UEBA_REGISTER_TOKEN = "ueba_token";
    public static final String UEBA_BLACKLIST_TYPE = "blacklist";
    public static final String UEBA_RULE_TEMPLATE_TYPE = "rule_template";
    public static final String UEBA_DATA_SOURCE_TYPE = "datasource";

    public static final String USER_ENTITY_NAMESPACE = "user>entity";
    public static final String USER_LOG_NAMESPACE = "user>log";
    public static final String TOTAL_LOG_COUNT = "total_analysed_log_count";
    public static final String TOTAL_USER_COUNT = "total_analysed_user_count";

    public static final String STREAM_LOG_ID_FIELD = "log_id";
    public static final String STREAM_TIME_FIELD = "occur_time";
    public static final String DATA_SOURCE_SCHEMA_FIELD = "schema";

    public static final String NULL_GROUP = "__NO_VALUE_KEY";

    public static final int DISPOSING = 0;
    public static final int PENDING = 1;
    public static final int CONFIRMED = 2;
    public static final int IGNORED = 3;

    public static final String SPLIT_SIGN = "@@";

    public static final String RULE_STAGE_NONE = "none";

    public static final String SCENARIO_PREVIEW_PREFIX = "PREV_";

    /**
     * white-list in scenario level
     */
    public static final String TEMP_WHITELIST_PREFIX = "TEMP_WHITELIST_";

    public static final String ALERT_FOLLOW_UP_ACTION_HISTORY = "action_history";
    public static final int ACTION_ENABLED = 0;
    public static final int ACTION_DISABLED = 1;
}
