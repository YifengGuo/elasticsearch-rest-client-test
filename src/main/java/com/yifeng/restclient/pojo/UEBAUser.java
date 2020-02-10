package com.yifeng.restclient.pojo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;

import java.util.ArrayList;
import java.util.List;

import static com.yifeng.restclient.pojo.UEBA.PENDING;


/**
 * used for taiping version user
 */
public class UEBAUser {

    @JSONField(name = "user_config_id")
    private String userConfigId;

    private String id;

    private String name;

    private String department;

    private String supervisor;

    private String email;

    private String phone;

    private String status;

    private String role;

    private String valid;

    @JSONField(name = "watch_time")
    private long watchTime;

    @JSONField(name = "hire_date")
    private long hireDate;

    @JSONField(name = "quit_date")
    private long quitDate;

    @JSONField(name = "asset_size")
    private long assetSize;

    @JSONField(name = "scenario_size")
    private long scenarioSize;

    @JSONField(name = "is_watch")
    private boolean watch;

    @JSONField(name = "scenarios_top3")
    private List<String> scenariosTop3;

    @JSONField(name = "alarm_level")
    private int alarmLevel;

    private List<String> scenarios;

    @JSONField(name = "event_size")
    private long eventSize;

    @JSONField(name = "first_opt_time")
    private long firstOptTime;

    @JSONField(name = "end_opt_time")
    private long endOptTime;

    private long score;

    @JSONField(name = "alert_top3")
    private List<String> alertTop3;

    @JSONField(name = "occur_time")
    private long occurTime;

    @JSONField(name = "alert_size")
    private long alertSize;

    private long modified;

    @JSONField(name = "asset_top3")
    private List<String> assetTop3;

    @JSONField(name = "entity_config_id")
    private String entityConfigId;

    @JSONField(name = "org_id")
    private String orgId;

    private String uid;

    @JSONField(name = "org_name_level2")
    private String orgNameLevel2;

    @JSONField(name = "orgfullname")
    private String orgFullName;

    @JSONField(name = "department_id")
    private String departmentId;

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getOrgNameLevel2() {
        return orgNameLevel2;
    }

    public void setOrgNameLevel2(String orgNameLevel2) {
        this.orgNameLevel2 = orgNameLevel2;
    }

    public String getOrgFullName() {
        return orgFullName;
    }

    public void setOrgFullName(String orgFullName) {
        this.orgFullName = orgFullName;
    }

    public String getDepartmentId() {
        return departmentId;
    }

    public void setDepartmentId(String departmentId) {
        this.departmentId = departmentId;
    }

    public String getUserConfigId() {
        return userConfigId;
    }

    public void setUserConfigId(String userConfigId) {
        this.userConfigId = userConfigId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public String getSupervisor() {
        return supervisor;
    }

    public void setSupervisor(String supervisor) {
        this.supervisor = supervisor;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getRole() {
        return role;
    }

    public String getValid() {
        return valid;
    }

    public void setValid(String valid) {
        this.valid = valid;
    }

    public long getHireDate() {
        return hireDate;
    }

    public void setHireDate(long hireDate) {
        this.hireDate = hireDate;
    }

    public long getQuitDate() {
        return quitDate;
    }

    public void setQuitDate(long quitDate) {
        this.quitDate = quitDate;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public long getAssetSize() {
        return assetSize;
    }

    public void setAssetSize(long assetSize) {
        this.assetSize = assetSize;
    }

    public long getScenarioSize() {
        return scenarioSize;
    }

    public void setScenarioSize(long scenarioSize) {
        this.scenarioSize = scenarioSize;
    }

    public boolean isWatch() {
        return watch;
    }

    public void setWatch(boolean watch) {
        this.watch = watch;
    }

    public List<String> getScenariosTop3() {
        return scenariosTop3;
    }

    public void setScenariosTop3(List<String> scenariosTop3) {
        this.scenariosTop3 = scenariosTop3;
    }

    public int getAlarmLevel() {
        return alarmLevel;
    }

    public void setAlarmLevel(int alarmLevel) {
        this.alarmLevel = alarmLevel;
    }

    public List<String> getScenarios() {
        return scenarios;
    }

    public void setScenarios(List<String> scenarios) {
        this.scenarios = scenarios;
    }

    public long getEventSize() {
        return eventSize;
    }

    public void setEventSize(long eventSize) {
        this.eventSize = eventSize;
    }

    public long getFirstOptTime() {
        return firstOptTime;
    }

    public void setFirstOptTime(long firstOptTime) {
        this.firstOptTime = firstOptTime;
    }

    public long getEndOptTime() {
        return endOptTime;
    }

    public void setEndOptTime(long endOptTime) {
        this.endOptTime = endOptTime;
    }

    public long getScore() {
        return score;
    }

    public void setScore(long score) {
        this.score = score;
    }

    public List<String> getAlertTop3() {
        return alertTop3;
    }

    public void setAlertTop3(List<String> alertTop3) {
        this.alertTop3 = alertTop3;
    }

    public long getOccurTime() {
        return occurTime;
    }

    public void setOccurTime(long occurTime) {
        this.occurTime = occurTime;
    }

    public long getAlertSize() {
        return alertSize;
    }

    public void setAlertSize(long alertSize) {
        this.alertSize = alertSize;
    }

    public long getModified() {
        return modified;
    }

    public void setModified(long modified) {
        this.modified = modified;
    }

    public List<String> getAssetTop3() {
        return assetTop3;
    }

    public void setAssetTop3(List<String> assetTop3) {
        this.assetTop3 = assetTop3;
    }

    public String getEntityConfigId() {
        return entityConfigId;
    }

    public void setEntityConfigId(String entityConfigId) {
        this.entityConfigId = entityConfigId;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public long getWatchTime() {
        return watchTime;
    }

    public void setWatchTime(long watchTime) {
        this.watchTime = watchTime;
    }

    public static UEBAUser createDefault() {
        UEBAUser user = new UEBAUser();
        user.setWatchTime(0L);
        user.setAlarmLevel(0);
        user.setAlertSize(0L);
        user.setAlertTop3(new ArrayList<>());
        user.setAssetSize(0L);
        user.setAssetTop3(new ArrayList<>());
        long now = System.currentTimeMillis();
        user.setFirstOptTime(now);
        user.setEndOptTime(now);
        user.setOccurTime(now);
        user.setModified(now);
        user.setEventSize(0L);
        user.setRole("");
        user.setScenarios(new ArrayList<>());
        user.setScenarioSize(0L);
        user.setScenariosTop3(new ArrayList<>());
        user.setScore(0L);
        user.setStatus(String.valueOf(PENDING));
        user.setWatch(false);
        return user;
    }

    /**
     * importUser的时候，如果用户存在，删除默认值，避免更新后的用户score等信息被重置
     */
    public String toUpdateUserString() {
        JSONObject jsonObject=JSONObject.parseObject(JSON.toJSONString(this));
        jsonObject.remove("alarm_level");
        jsonObject.remove("alert_size");
        jsonObject.remove("alert_top3");
        jsonObject.remove("asset_size");
        jsonObject.remove("asset_top3");
        jsonObject.remove("end_opt_time");
        jsonObject.remove("event_size");
        jsonObject.remove("first_opt_time");
        jsonObject.remove("is_watch");
        jsonObject.remove("scenario_size");
        jsonObject.remove("occur_time");
        jsonObject.remove("scenarios");
        jsonObject.remove("scenarios_top3");
        jsonObject.remove("score");
        jsonObject.remove("watch_time");
        return jsonObject.toJSONString();
    }
}
