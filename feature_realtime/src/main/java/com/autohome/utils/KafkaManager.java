package com.autohome.utils;

import com.autohome.sources.KafkaServers;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * @program: PJ_DATACENTER_DW_UAS
 * @description: kafka propertios
 * @author: rj.Guo
 * @create: 2019-11-07 17:39
 **/
public class KafkaManager {

    public static Properties getKafkaProperties(KafkaServers kafkaServers) {
        Properties properties = new Properties();
        setDefaultProperties(properties);
        properties.setProperty("bootstrap.servers", kafkaServers.getIp());
        properties.put("client.id", kafkaServers.getToken());
        properties.put("topic.id", kafkaServers.getRealName());
        properties.put("request.timeout.ms", 360000);
        return properties;
    }

    @Deprecated
    public static Properties getKafkaProperties(String topic) {
        Properties properties = new Properties();
        setDefaultProperties(properties);
        properties.setProperty("bootstrap.servers", KafkaServers.getIp(topic));
        properties.put("client.id", KafkaServers.getToken(topic));
        properties.put("topic.id", topic);
        return properties;
    }

    public static Properties getKafkaConsumerProperties(KafkaServers kafkaServers,String groupId) {
        Properties properties = new Properties();
        setDefaultProperties(properties);
        properties.setProperty("bootstrap.servers",kafkaServers.getIp());
        properties.put("client.id", kafkaServers.getToken());
        properties.put("topic.id", kafkaServers.getRealName());
        properties.put("group.id",groupId);
        properties.put("max.partition.fetch.bytes", 10 * 1024 * 1024);
        properties.put("max.poll.records", 500);
        return properties;
    }

    public static Properties getKafkaProducerProperties(KafkaServers kafkaServers,String groupId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaServers.getIp());
        properties.put("client.id",kafkaServers.getToken());
        properties.put("retries", 2); // 发送失败的最大尝试次数
        properties.put("batch.size", "1048576"); // 1MB
        //properties.put("compression.type", "gzip");
        properties.put("linger.ms", "5"); // 最长延迟5秒必须发送
        properties.put("buffer.memory", "67108864");// 64MB
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return properties;
    }

    private static void setDefaultProperties(Properties properties) {
        properties.put("batch.size", 5024000); //5MB
        properties.put("compression.type", "gzip");// gz压缩
        properties.put("linger.ms", 20); //最长延迟20ms必须发送
        properties.put("buffer.memory", 268435456);//64MB
        properties.put("max.request.size", 5242880);
        properties.put("request.timeout.ms", 600000);
        properties.put("retries", 10);
        properties.put("enable.auto.commit", false);
        properties.put("max.block.ms",300000);
    }

    public static void sendMsgFromString(KafkaServers kafkaServers,String groupId, String data) throws Exception{
        List<String> list = new LinkedList<String>(){{
            add(data);
        }};
        sendMsgFromList(kafkaServers, groupId, list);
    }

    public static void sendMsgFromList(KafkaServers kafkaServers, String groupId, List<String> data){
        KafkaProducer producer = null;
        try{
            producer = new KafkaProducer(KafkaManager.getKafkaConsumerProperties(kafkaServers, groupId));
            for (int i =0; i<data.size(); i++){
                ProducerRecord<String, String> message = new ProducerRecord<>(kafkaServers.getTopicName(), data.get(i));
                producer.send(message, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.out.println("send message failed with " + exception.getMessage());
                        } else {
                            System.out.println("message sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());

                        }
                    }});
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            if(Objects.nonNull(producer)) {
                producer.close();
            }
        }
    }

}
