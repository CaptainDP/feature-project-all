package com.autohome.schemas;

import com.alibaba.fastjson.JSONObject;
import com.autohome.models.*;
import com.autohome.sources.KafkaServers;
import com.google.protobuf.Message;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @ClassName NlpScheme
 * @Description TODO
 * @Author chenhc
 * @Date 2020/06/05 12:22
 **/

public class ResouecePoolPBScheme implements KafkaDeserializationSchema<JSONObject> {
    String topic;

    @Override
    public boolean isEndOfStream(JSONObject object) {
        return false;
    }

    @Override
    public JSONObject deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        long timestamp = consumerRecord.timestamp();

        byte[] bytes = consumerRecord.value();
        Message message = null;
        String object_uid = null;
        if(topic.equals(KafkaServers.lf_zhengpaiku_tb_active_pool_tidb.getRealName())){
            ActiveModel.Active active = ActiveModel.Active.parseFrom(bytes);
            object_uid = active.getObjectUid();
            message = active;
        }else if(topic.equals(KafkaServers.lf_zhengpaiku_tb_car_pool_tidb.getRealName())){
            CarModel.Car car = CarModel.Car.parseFrom(bytes);
            object_uid = car.getObjectUid();
            message = car;
        }else if(topic.equals(KafkaServers.lf_zhengpaiku_tb_richmedia_pool_tidb.getRealName())){
            RichMediaModel.RichMedia richMedia = RichMediaModel.RichMedia.parseFrom(bytes);
            object_uid = richMedia.getObjectUid();
            message = richMedia;
        }else if(topic.equals(KafkaServers.lf_zhengpaiku_tb_user_pool_tidb.getRealName())){
            UserModel.User user = UserModel.User.parseFrom(bytes);
            object_uid = user.getObjectUid();
            message = user;
        }else if(topic.equals(KafkaServers.lf_zhengpaiku_tb_topic_pool_tidb.getRealName())){
            TopicModel.Topic topic = TopicModel.Topic.parseFrom(bytes);
            object_uid = topic.getObjectUid();
            message = topic;
        }else{
            message = null;
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("timestamp", timestamp);
        jsonObject.put("pb", message);
        jsonObject.put("object_uid", object_uid);
        jsonObject.put("topic", topic);
        return jsonObject;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(new TypeHint<JSONObject>() {
            @Override
            public TypeInformation<JSONObject> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
    }

    public ResouecePoolPBScheme(String topic) {
        this.topic = topic;
    }

}
