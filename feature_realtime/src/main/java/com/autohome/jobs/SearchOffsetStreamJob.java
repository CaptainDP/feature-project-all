package com.autohome.jobs;

import com.alibaba.fastjson.JSONObject;
import com.autohome.beans.Segement;
import com.autohome.beans.SourceBean;
import com.autohome.maps.PB2JsonMap;
import com.autohome.models.OffsetModel;
import com.autohome.schemas.ResouecePoolPBScheme;
import com.autohome.sinks.RedisSink;
import com.autohome.sources.KafkaServers;
import com.autohome.utils.ESHttpClientUtils;
import com.autohome.utils.FlinkUtils;
import com.autohome.utils.KafkaManager;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SearchOffsetStreamJob {
	private static Logger logger = LoggerFactory.getLogger("SearchOffsetStreamJob");

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

		StreamExecutionEnvironment env = FlinkUtils.getOnlineStreamEnv(TimeCharacteristic.ProcessingTime);

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
		env.addSource(pool_consumer)
			.filter(x -> x != null)
			.map(new PB2JsonMap()).name(kafkaServer.getTopicName() + kafkaServer.getRealName())
			.map(new MapFunction<SourceBean, JSONObject>() {
				@Override
				public JSONObject map(SourceBean input) throws Exception {
					String author = input.getAuthor();
					String content = input.getContent();
					String stitle = input.getStitle();
					String title = input.getTitle();
					List<Segement> authorSegList = ESHttpClientUtils.post(author);
					List<Segement> contentSegList = ESHttpClientUtils.post(content);
					List<Segement> stitleSegList = ESHttpClientUtils.post(stitle);
					List<Segement> titleSegList = ESHttpClientUtils.post(title);

					OffsetModel.Offset.Builder offset = OffsetModel.Offset.newBuilder();
					titleSegList.stream().map(x -> offset.putTitleTermList(x.getToken(), (int) x.getStart_offset()));
					stitleSegList.stream().map(x -> offset.putStitleTermList(x.getToken(), (int) x.getStart_offset()));
					authorSegList.stream().map(x -> offset.putAuthorTermList(x.getToken(), (int) x.getStart_offset()));
					contentSegList.stream().map(x -> offset.putContentTermList(x.getToken(), (int) x.getStart_offset()));

					JSONObject output = new JSONObject().fluentPut("offset", offset)
							.fluentPut("item_key", input.getItem_key());
					return output;
				}
			}).name("query term offset")
			.addSink(new RedisSink()).name("pushRedis");
	}

}
