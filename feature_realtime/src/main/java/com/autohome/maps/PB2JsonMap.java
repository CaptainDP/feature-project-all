package com.autohome.maps;

import com.alibaba.fastjson.JSONObject;
import com.autohome.beans.SourceBean;
import com.autohome.models.*;
import com.autohome.sources.KafkaServers;
import com.autohome.utils.RtypeTools;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName PB2JsonMap
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/05 18:54
 **/

public class PB2JsonMap extends RichMapFunction<JSONObject,SourceBean> {
    private Map<String, String> rTypeMap = Maps.newHashMap();
    private transient ScheduledExecutorService service;

    @Override
    public void open(Configuration parameters) throws Exception {
        service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                rTypeMap = RtypeTools.getRTypeData();
            }
        }, 60, 60, TimeUnit.MINUTES);


    }

    @Override
    public SourceBean map(JSONObject input) throws Exception {
        Message pb = (Message)input.get("pb");
        byte[] bytes = pb.toByteArray();

        SourceBean result = new SourceBean();
        String topic = input.getString("topic");
        Long timestamp = input.getLong("timestamp");
        result.setTopic(topic);
        result.setTimestamp(timestamp);

        if(topic.equals(KafkaServers.lf_zhengpaiku_tb_active_pool_tidb.getRealName())){
            ActiveModel.Active active = ActiveModel.Active.parseFrom(bytes);
            result.setTitle(active.getTitle());
            result.setStitle("");
            result.setAuthor("");
            result.setContent(active.getContent());
            result.setItem_key(getItem_key(active.getBizType(),active.getBizId()));
        }else if(topic.equals(KafkaServers.lf_zhengpaiku_tb_car_pool_tidb.getRealName())){
            CarModel.Car car = CarModel.Car.parseFrom(bytes);
            result.setTitle(car.getTitle());
            result.setStitle("");
            result.setAuthor("");
            result.setContent(car.getContent());
            result.setItem_key(getItem_key(car.getBizType(),car.getBizId()));
        }else if(topic.equals(KafkaServers.lf_zhengpaiku_tb_richmedia_pool_tidb.getRealName())){
            RichMediaModel.RichMedia richMedia = RichMediaModel.RichMedia.parseFrom(bytes);
            result.setTitle(richMedia.getTitle());
            result.setStitle(richMedia.getLongTitle());
            result.setAuthor(richMedia.getAuthor());
            result.setContent(richMedia.getContent());
            result.setItem_key(getItem_key(richMedia.getBizType(),richMedia.getBizId()));
        }else if(topic.equals(KafkaServers.lf_zhengpaiku_tb_user_pool_tidb.getRealName())){
            UserModel.User user = UserModel.User.parseFrom(bytes);
            result.setTitle(user.getTitle());
            result.setStitle("");
            result.setAuthor(user.getAuthor());
            result.setContent(user.getContent());
            result.setItem_key(getItem_key(user.getBizType(),user.getBizId()));
        }else if(topic.equals(KafkaServers.lf_zhengpaiku_tb_topic_pool_tidb.getRealName())){
            TopicModel.Topic topicP = TopicModel.Topic.parseFrom(bytes);
            result.setTitle(topicP.getTitle());
            result.setStitle(topicP.getLongTitle());
            result.setAuthor(topicP.getAuthor());
            result.setContent(topicP.getContent());
            result.setItem_key(getItem_key(topicP.getBizType(),topicP.getBizId()));
        }
        return result;
    }

    /***
     * 通过objectUid获取itemKey
     * @param biz_type
     * @param biz_id
     * @return
     */
    public String getItem_key(String biz_type,String biz_id){
        return rTypeMap.get(biz_type)+"-"+biz_id;
    }

    @Override
    public void close() throws Exception {
        service.shutdown();
    }
}
