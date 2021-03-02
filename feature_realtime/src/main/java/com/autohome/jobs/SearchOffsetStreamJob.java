package com.autohome.jobs;

import com.alibaba.fastjson.JSONObject;
import com.autohome.maps.PB2JsonMap;
import com.autohome.maps.PbHandlerMap;
import com.autohome.maps.SearchDataFileter;
import com.autohome.models.*;
import com.autohome.schemas.ResouecePoolPBScheme;
import com.autohome.sinks.RedisSink;
import com.autohome.sources.KafkaServers;
import com.autohome.utils.FlinkUtils;
import com.autohome.utils.KafkaManager;
import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchOffsetStreamJob {
	private static Logger logger = LoggerFactory.getLogger("SearchOffsetStreamJob");

	private static String jieSoServerFlag = "es";

	public static void main(String[] args) throws Exception {
		MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

		String groupid = "search_offset_stream_job_consumer";
		if(params.has("groupid")){
			groupid = params.get("groupid");
		}
		long startTimeStamp = 0;
		if(params.has("startTimeStamp")){
			startTimeStamp = params.getLong("startTimeStamp");
		}

		if(params.has("jieSoServerFlag")){
			jieSoServerFlag = params.get("jieSoServerFlag");
		}

		logger.info("jieSoServerFlag:{}", jieSoServerFlag);

		StreamExecutionEnvironment env = FlinkUtils.getOnlineStreamEnv(TimeCharacteristic.ProcessingTime);

		env.disableOperatorChaining();

		env.getConfig().registerTypeWithKryoSerializer(CarModel.Car.class, ProtobufSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(RichMediaModel.RichMedia.class, ProtobufSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(ActiveModel.Active.class, ProtobufSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(UserModel.User.class, ProtobufSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(TopicModel.Topic.class, ProtobufSerializer.class);
		env.getConfig().registerTypeWithKryoSerializer(OffsetModel.Offset.class, ProtobufSerializer.class);


		//car
		KafkaServers car_pool = KafkaServers.lf_zhengpaiku_tb_car_pool_tidb;
		commonLogic(env, car_pool, groupid,startTimeStamp);

		//active
		KafkaServers active_pool = KafkaServers.lf_zhengpaiku_tb_active_pool_tidb;
		commonLogic(env, active_pool, groupid,startTimeStamp);

		//richmedia
		KafkaServers richmedia_pool = KafkaServers.lf_zhengpaiku_tb_richmedia_pool_tidb;
		commonLogic(env, richmedia_pool, groupid,startTimeStamp);

		//user
		KafkaServers user_pool = KafkaServers.lf_zhengpaiku_tb_user_pool_tidb;
		commonLogic(env, user_pool, groupid,startTimeStamp);

		//topic
		KafkaServers topic_pool = KafkaServers.lf_zhengpaiku_tb_topic_pool_tidb;
		commonLogic(env, topic_pool, groupid,startTimeStamp);

		env.execute("Streaming SearchOffsetStreamJob");
	}
	public static void commonLogic(StreamExecutionEnvironment env,KafkaServers kafkaServer,String groupid,long startTimeStamp) throws Exception {
		FlinkKafkaConsumer010<JSONObject> pool_consumer = new FlinkKafkaConsumer010<>(kafkaServer.getRealName(), new ResouecePoolPBScheme(kafkaServer.getRealName()), KafkaManager.getKafkaConsumerProperties(kafkaServer, groupid));
		if(startTimeStamp>0){
			logger.info("设置从指定时间开始消费:{}",startTimeStamp);
			pool_consumer.setStartFromTimestamp(startTimeStamp);
		}
		env.addSource(pool_consumer).name(kafkaServer.getRealName())
			.map(new PB2JsonMap()).name("to_Field_bean")
			.filter(new SearchDataFileter()).name("filter_search_data")
			.map(new PbHandlerMap(jieSoServerFlag)).name("query_term_offset")
			.addSink(new RedisSink()).name("push_redis");
	}

}
