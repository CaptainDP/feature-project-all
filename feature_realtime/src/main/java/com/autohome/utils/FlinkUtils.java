package com.autohome.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @program: udf
 * @description: 通用工具类
 * @author: rj.Guo
 * @create: 2018-11-28 19:14
 **/
public class FlinkUtils {

    /**
     * 生成kafka proerties
     *
     * @return java.util.Properties
     * @Author rujie.guo
     * @Description //TODO
     * @Date 6:08 PM 2018/12/27
     * @Param [topic, args{group.id,bootstart.servers,cliend.id}]
     **/
    public static Properties genProperty(String topic, String... args) {
        //kafka properties
        Properties properties = new Properties();
        //local
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "1");
        properties.setProperty("topic", topic);
        properties.setProperty("max.partition.fetch.bytes", "4194304");
        if (args.length > 0) {
            properties.setProperty("group.id", args[0]);
        }
        if (args.length > 1) {
            properties.setProperty("bootstrap.servers", args[1]);
        }
        if (args.length > 2) {
            properties.setProperty("client.id", "16b0104593044c2b8b134caec6e48c57");
            if (!StringUtils.isBlank(args[2])) {
                properties.setProperty("client.id", args[2]);
            }
        }
        return properties;
    }


    /**
     * 获取运行时环境
     *
     * @return org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
     * @Author rujie.guo
     * @Description //TODO
     * @Date 8:09 PM 2018/12/27
     * @Param [type]
     **/
    public static StreamExecutionEnvironment getOnlineStreamEnv(TimeCharacteristic type) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 打印日志
        env.getConfig().enableSysoutLogging();
        // 窗口时间
        env.setStreamTimeCharacteristic(type);
        // 硬盘快照备份
        setDisasterRecoveryCheckpointLocation(env);
//        //rocksdb 备份
//        RocksDBStateBackend backend = new RocksDBStateBackend("hdfs://10.26.240.70:8020/user/olap/checkpoints-data", true);
//        env.setStateBackend(backend);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.MINUTES)));
        setCheckPointConfig(env);
        return env;
    }

    /**
     * 发布到1.9.2 apollo集群，flink平台要求修改checkpoit 路径
     */
    public static StreamExecutionEnvironment setDisasterRecoveryCheckpointLocation(StreamExecutionEnvironment env) {
        String platformId = System.getProperty("platformId");
        //线上
        if (platformId == null) {
            platformId = "fatal";
        }
        StateBackend stateBackend = new FsStateBackend("hdfs://ns1/user/flink/checkpoints-data/" + platformId);
        env.setStateBackend(stateBackend);
        return env;
    }

    protected static void setCheckPointConfig(StreamExecutionEnvironment executionEnvironment) {
        executionEnvironment.enableCheckpointing(30000L);
        CheckpointConfig checkpointConfig = executionEnvironment.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(executionEnvironment.getCheckpointInterval());
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointInterval(1000 * 60);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000 * 60);
    }

    /**
     * 获取运行时环境
     *
     * @return org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
     * @Author rujie.guo
     * @Description //TODO
     * @Date 8:09 PM 2018/12/27
     * @Param [type]
     **/
    public static StreamExecutionEnvironment getLocalStreamEnv(TimeCharacteristic type) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableSysoutLogging();
        env.setStreamTimeCharacteristic(type); //设置窗口的时间单位为process time
        return env;
    }
}
