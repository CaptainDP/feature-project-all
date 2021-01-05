package com.autohome.utils;

import com.autohome.commons.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @ClassName RedisUtils
 * @Description TODO
 * @Author chenhc
 * @Date 2020/08/19 17:25
 **/

public class RedisUtils {
    private static Logger logger = LoggerFactory.getLogger(RedisUtils.class);
    private static JedisPool jedisPool = null;
    private static JedisPoolConfig jedisPoolConfig;
    static {
        jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(3000);
        jedisPoolConfig.setMaxIdle(100);
        jedisPoolConfig.setMaxWaitMillis(5000);
        jedisPoolConfig.setTestOnBorrow(true);//jedis 第一次启动时，会报错
        jedisPoolConfig.setTestOnReturn(true);
        jedisPool = new JedisPool(jedisPoolConfig, Constant.REDIS_HOST, Constant.REDIS_PORT, 2000, Constant.REDIS_PASSWD, 0);
    }
    public static Jedis getClient(){
        //logger.info(jedisPoolConfig.getMaxTotal()+","+jedisPoolConfig.getMaxIdle()+","+jedisPool.getNumActive()+","+jedisPool.getNumIdle()+","+jedisPool.getNumWaiters());
        return jedisPool.getResource();
    }
    public static void destory(){
        jedisPool.destroy();
    }
}
