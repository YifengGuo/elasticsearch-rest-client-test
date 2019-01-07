package com.yifeng.restclient.utils.request_sender;

/**
 * Created by guoyifeng on 12/12/18
 */

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;


/**
 * This class works as helper class for {@link NewAggregationRequestGenerator} <br>
 */
public class CustomAggregationBuilder {

    private static Logger LOG = LoggerFactory.getLogger(NewAggregationRequestGenerator.class);

    private JSONObject aggregationBuilder;

    private List<String> nameList;  // to record names for each aggregation task

    private AtomicInteger offset;  // to record current agg name position in the nameList

    private boolean configComplete;  // to represent if settings for current agg task is done or not, if not, current setting must be complete before setting next agg task

    private boolean hasName;

    private boolean hasType;

    private JSONObject currTask;

    private String currType;

    public CustomAggregationBuilder() {
        this.aggregationBuilder = new JSONObject();
        nameList = new ArrayList<>();
        offset = new AtomicInteger(0);
        configComplete = true;  // initially, without any task naming, this should be set to true
        hasName = false;
        hasType = false;
    }

    /**
     * name this aggregation <br>
     * @param name the name for this aggregation
     * @return aggregationBuilder
     */
    public CustomAggregationBuilder setAggName(String name) {
        if (!configComplete) throw new IllegalStateException("Please complete current agg task settings before setting name for next agg task.");
        this.aggregationBuilder.put(name, new JSONObject());
        hasName = true;
        nameList.add(name);
        configComplete = false;  // settings begin
        return this;
    }

    /**
     * set type for the aggregation, if name has not been set, this will throw exception
     * @param type the type of metrics aggregation. eg: "terms", "cardinality", etc
     * @return aggregationBuilder
     */
    public CustomAggregationBuilder setAggType(String type) {
        if (!hasName) {
            throw new IllegalStateException("Please set name before setting aggregation type");
        }
        // get current agg task JSONObject by offset in nameList
        currTask = this.aggregationBuilder.getJSONObject(nameList.get(offset.get()));

        // set type for current task
        currTask.put(type, new JSONObject());

        hasType = true;
        currType = type;
        return this;
    }

    public CustomAggregationBuilder setFieldName(String field) {
        if (!hasType) throw new IllegalStateException("Please set type before setting aggregation field.");
        currTask.getJSONObject(currType).put("field", field);
        return this;
    }

    public CustomAggregationBuilder setSize(int size) {
        if (!hasType) throw new IllegalStateException("Please set type before setting aggregation size.");
        if (currType.equals("cardinality")) return this;  // for cardinality, size is redundant
        currTask.getJSONObject(currType).put("size", size);
        return this;
    }

    public CustomAggregationBuilder setInterval(String interval) {
        if (!hasType) throw new IllegalStateException("Please set type before setting aggregation interval.");
        if (!currType.equals("date_histogram")) throw new IllegalArgumentException("interval can only be set for date_histogram");
        currTask.getJSONObject(currType).put("interval", interval);
        return this;
    }

    public CustomAggregationBuilder setOrder(JSONObject obj, Consumer<JSONObject> consumer) {
        if (!hasType) throw new IllegalStateException("Please set type before setting aggregation order.");
        consumer.accept(obj);
        currTask.getJSONObject(currType).put("order", obj);
        return this;
    }

    public CustomAggregationBuilder setIncludeExclude(String[] include, String[] exclude) {
        if (!hasType) throw new IllegalStateException("Please set type before setting aggregation include and exclude.");
        if (!currType.equals("terms")) throw new IllegalArgumentException("include and exclude can only be set for terms");
        currTask.getJSONObject(currType).put("include", include);
        currTask.getJSONObject(currType).put("exclude", exclude);
        return this;
    }

    public CustomAggregationBuilder setFrom(int from) {
        if (!hasType) throw new IllegalStateException("Please set type before setting aggregation from position.");
        currTask.getJSONObject(currType).put("from", from);
        return this;
    }

    /**
     *
     * @param sourceFlag If _source is requested then just the part of the source of the nested object is returned, not the entire source of the document.
     * @return
     */
    public CustomAggregationBuilder fetchSource(boolean sourceFlag) {
        if (!hasType) throw new IllegalStateException("Please set type before setting aggregation source flag.");
        currTask.getJSONObject(currType).put("_source", sourceFlag);
        return this;
    }

    // represent current agg task settings process is complete
    public CustomAggregationBuilder complete() {
        hasName = false;
        hasType = false;
        configComplete = true;
        offset.incrementAndGet();
        return this;
    }

    public CustomAggregationBuilder setSubaggregation(CustomAggregationBuilder subAgg) {
        this.currTask.put("aggregations", subAgg.getAggregationBuilder());
        return this;
    }

    public JSONObject getAggregationBuilder() {
        return aggregationBuilder;
    }

//    public JSONObject doReturn() {
//        if (configComplete) {
//            return this.aggregationBuilder;
//        } else {
//            throw new IllegalStateException("Please complete current agg task settings before return JSONObject.");
//        }
//    }
}
