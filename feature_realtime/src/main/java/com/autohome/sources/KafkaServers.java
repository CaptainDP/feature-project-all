package com.autohome.sources;

/**
 * @program: PJ_DATACENTER_DW_UAS
 * @description:
 * @author: rj.Guo
 * @create: 2019-11-07 17:51
 **/
public enum KafkaServers {

    lf_zhengpaiku_tb_active_pool_tidb("10.28.5.87:9092,10.28.5.88:9092,10.28.5.89:9092","a5e3649821a24ce7991ac11bdbe8842f","正排库active大表topic-廊坊","zhengpaiku_tb_active_pool_tidb"),
    lf_zhengpaiku_tb_car_pool_tidb("10.28.5.87:9092,10.28.5.88:9092,10.28.5.89:9092","a5e3649821a24ce7991ac11bdbe8842f","正排库car大表topic-廊坊","zhengpaiku_tb_car_pool_tidb"),
    lf_zhengpaiku_tb_poi_pool_tidb("10.28.5.87:9092,10.28.5.88:9092,10.28.5.89:9092","a5e3649821a24ce7991ac11bdbe8842f","正排库poi大表topic-廊坊","zhengpaiku_tb_poi_pool_tidb"),
    lf_zhengpaiku_tb_richmedia_pool_tidb("10.28.5.87:9092,10.28.5.88:9092,10.28.5.89:9092","a5e3649821a24ce7991ac11bdbe8842f","正排库richmedia大表topic-廊坊","zhengpaiku_tb_richmedia_pool_tidb"),
    lf_zhengpaiku_tb_topic_pool_tidb("10.28.5.87:9092,10.28.5.88:9092,10.28.5.89:9092","a5e3649821a24ce7991ac11bdbe8842f","正排库topic大表topic-廊坊", "zhengpaiku_tb_topic_pool_tidb"),
    lf_zhengpaiku_tb_user_pool_tidb("10.28.5.87:9092,10.28.5.88:9092,10.28.5.89:9092","a5e3649821a24ce7991ac11bdbe8842f","正排库user大表topic-廊坊","zhengpaiku_tb_user_pool_tidb");

    private String ip;
    private String token;
    private String topicName;
    private String realName;

    KafkaServers(String ...args) {
        this.ip = args[0];
        this.token = args[1];
        this.topicName = args[2];
        if(args.length>3){
            this.realName = args[3];
        }

    }

    public static String getIp(String topic) {
        return KafkaServers.valueOf(topic).ip;
    }
    public static String getToken(String topic) {
        return KafkaServers.valueOf(topic).token;
    }
    public static String getTopic(String topic) {
        return KafkaServers.valueOf(topic).topicName;
    }

    public String getIp() {
        return ip;
    }

    public String getToken() {
        return token;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getRealName() {
        return realName;
    }

    public static String getRealName(String topic) {
        return KafkaServers.valueOf(topic).realName;
    }
}
