package com.autohome.sinks;

import com.alibaba.fastjson.JSONObject;
import com.autohome.commons.Constant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @ClassName RedisSink
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/05 19:59
 **/

public class RedisSink extends RichSinkFunction<JSONObject> {
    private static Logger logger = LoggerFactory.getLogger("RedisSink");
    private static JedisPool jedisPool = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(3000);
        jedisPoolConfig.setMaxIdle(100);
        jedisPoolConfig.setMaxWaitMillis(5000);
        jedisPoolConfig.setTestOnBorrow(true);//jedis 第一次启动时，会报错
        jedisPoolConfig.setTestOnReturn(true);
        jedisPool = new JedisPool(jedisPoolConfig, Constant.REDIS_HOST, Constant.REDIS_PORT, 2000, Constant.REDIS_PASSWD, 0);
    }

    @Override
    public void close() throws Exception {
        jedisPool.destroy();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        Jedis redis = null;
        try{
            redis = jedisPool.getResource();
            String status = redis.set(value.getString("item_key").getBytes(), value.getBytes("offset"));
            if(!"OK".equals(status))
                logger.error("push:{} status:{}",value.getString("item_key"),status);
        }catch (Exception e){
        }finally {
            redis.close();
        }

    }
}
