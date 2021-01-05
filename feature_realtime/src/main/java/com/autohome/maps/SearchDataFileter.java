package com.autohome.maps;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName SearchDataFileter
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/05 20:24
 **/

public class SearchDataFileter extends RichFilterFunction<JSONObject> {

    private static Logger logger = LoggerFactory.getLogger("SearchDataFileter");

    @Override
    public boolean filter(JSONObject input) throws Exception {
        if (input == null)
            return false;
        if (input.get("pb") == null)
            return false;
        Message pb = (Message) input.get("pb");
        String pbJsonStr = JsonFormat.printToString(pb);
        if (StringUtils.isBlank(pbJsonStr))
            return false;
        JSONObject inputJSON = JSONObject.parseObject(pbJsonStr);
        String biz_type = inputJSON.getString("biz_type");
        if (!"1".equals(input.getString("searchUsed")))
            return false;
        String jsonReserveStr = input.getString("jsonReserve");
        if (jsonReserveStr != null) {
            try {
                JSONObject jsonReserve = JSONObject.parseObject(jsonReserveStr);
                if (jsonReserve.getString("upstream").equals("nlp"))
                    return false;
            } catch (Exception e) {
                logger.error("jsonReserve parse json error data:{}", jsonReserveStr);
            }
        }
        return true;
    }
}
