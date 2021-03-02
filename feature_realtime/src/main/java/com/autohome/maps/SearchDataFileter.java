package com.autohome.maps;

import cn.hutool.crypto.digest.MD5;
import com.alibaba.fastjson.JSONObject;
import com.autohome.beans.SourceBean;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @ClassName SearchDataFileter
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/05 20:24
 **/

public class SearchDataFileter extends RichFilterFunction<SourceBean> {

    private static Logger logger = LoggerFactory.getLogger("SearchDataFileter");

    Map<String, Long> dupMap = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //增加本地缓存来降低重复数据的处理
        dupMap = new HashMap<>();
        Timer timer = new Timer("cache_clear_thread");
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                logger.info("cache clear" );

                for(Map.Entry<String,Long> item:dupMap.entrySet()){
                    if(System.currentTimeMillis()-item.getValue()>=3600000){
                        dupMap.remove(item.getKey());
                    }
                }
                dupMap.clear();
            }
        }, 3600000, 3600000);
    }

    @Override
    public boolean filter(SourceBean input) throws Exception {
        if (input == null)
            return false;

        if (!"1".equals(input.getSearchUsed())) {
            //logger.info("skip not search data");
            return false;
        }
        String jsonReserveStr = input.getJsonReserve();
        if (jsonReserveStr != null) {
            try {
                JSONObject jsonReserve = JSONObject.parseObject(jsonReserveStr);
                if (jsonReserve.getString("upstream").equals("nlp")) {
                    logger.info("skip upstream nlp data");
                    return false;
                }
            } catch (Exception e) {
                logger.error("jsonReserve parse json error data:{}", jsonReserveStr,e);
            }
        }
        StringBuffer sb = new StringBuffer(200);
        StringBuffer key = sb.append(input.getItem_key()).append("|").append(input.getTitle()).append("|").append(input.getStitle()).append("|").append(input.getAuthor()).append("|").append(input.getContent());
        String md5Key = MD5.create().digestHex(key.toString());
        if(dupMap.containsKey(md5Key)) {
            logger.info("dup item_key:{} key {}", input.getItem_key(),md5Key);
            return false;
        }
        dupMap.put(md5Key, System.currentTimeMillis());
        return true;
    }
}
