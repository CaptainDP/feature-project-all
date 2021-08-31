package com.captain.bigdata.taichi.demo.sql

object VideoFeatureColumnV2Sql {

  val sql =
    """select
      |device_id,
      |biz_type,
      |biz_id,
      |pvid,
      |posid,
      |
      |--pv-uv
      |hour(get_json_object(msg, '$.start_time')) as context_event_hour,
      |get_json_object(msg, '$.itemFeature.pv_1d') as pv_1d,
      |get_json_object(msg, '$.itemFeature.pv_3d') as pv_3d,
      |get_json_object(msg, '$.itemFeature.pv_7d') as pv_7d,
      |get_json_object(msg, '$.itemFeature.pv_15d') as pv_15d,
      |get_json_object(msg, '$.itemFeature.pv_30d') as pv_30d,
      |get_json_object(msg, '$.itemFeature.uv_1d') as uv_1d,
      |get_json_object(msg, '$.itemFeature.uv_3d') as uv_3d,
      |get_json_object(msg, '$.itemFeature.uv_7d') as uv_7d,
      |get_json_object(msg, '$.itemFeature.uv_15d') as uv_15d,
      |get_json_object(msg, '$.itemFeature.uv_30d') as uv_30d,
      |
      |--click_pv
      |get_json_object(msg, '$.itemFeature.click_pv_1d') as click_pv_1d,
      |get_json_object(msg, '$.itemFeature.click_pv_3d') as click_pv_3d,
      |get_json_object(msg, '$.itemFeature.click_pv_7d') as click_pv_7d,
      |get_json_object(msg, '$.itemFeature.click_pv_15d') as click_pv_15d,
      |get_json_object(msg, '$.itemFeature.click_pv_30d') as click_pv_30d,
      |
      |--sight_pv
      |get_json_object(msg, '$.itemFeature.sight_pv_1d') as sight_pv_1d,
      |get_json_object(msg, '$.itemFeature.sight_pv_3d') as sight_pv_3d,
      |get_json_object(msg, '$.itemFeature.sight_pv_7d') as sight_pv_7d,
      |get_json_object(msg, '$.itemFeature.sight_pv_15d') as sight_pv_15d,
      |get_json_object(msg, '$.itemFeature.sight_pv_30d') as sight_pv_30d,
      |
      |
      |--collect_cnt
      |get_json_object(msg, '$.itemFeature.collect_cnt_1d') as collect_cnt_1d,
      |get_json_object(msg, '$.itemFeature.collect_cnt_3d') as collect_cnt_3d,
      |get_json_object(msg, '$.itemFeature.collect_cnt_7d') as collect_cnt_7d,
      |get_json_object(msg, '$.itemFeature.collect_cnt_15d') as collect_cnt_15d,
      |get_json_object(msg, '$.itemFeature.collect_cnt_30d') as collect_cnt_30d,
      |
      |--click_uv
      |get_json_object(msg, '$.itemFeature.click_uv_1d') as click_uv_1d,
      |get_json_object(msg, '$.itemFeature.click_uv_3d') as click_uv_3d,
      |get_json_object(msg, '$.itemFeature.click_uv_7d') as click_uv_7d,
      |get_json_object(msg, '$.itemFeature.click_uv_15d') as click_uv_15d,
      |get_json_object(msg, '$.itemFeature.click_uv_30d') as click_uv_30d,
      |
      |
      |--like
      |get_json_object(msg, '$.itemFeature.like_cnt_1d') as like_cnt_1d,
      |get_json_object(msg, '$.itemFeature.like_cnt_3d') as like_cnt_3d,
      |get_json_object(msg, '$.itemFeature.like_cnt_7d') as like_cnt_7d,
      |get_json_object(msg, '$.itemFeature.like_cnt_15d') as like_cnt_15d,
      |get_json_object(msg, '$.itemFeature.like_cnt_30d') as like_cnt_30d,
      |
      |
      |--prop_author
      |get_json_object(msg, '$.itemFeature.fans_num') as fans_num,
      |get_json_object(msg, '$.itemFeature.works_num') as works_num,
      |get_json_object(msg, '$.itemFeature.comment_cnt_90d') as comment_cnt_90d,
      |get_json_object(msg, '$.itemFeature.favorites_cnt_90d') as favorites_cnt_90d,
      |get_json_object(msg, '$.itemFeature.share_cnt_90d') as share_cnt_90d,
      |get_json_object(msg, '$.itemFeature.like_cnt_90d') as like_cnt_90d,
      |
      |
      |--time
      |get_json_object(msg, '$.startTime') as start_time,
      |get_json_object(msg, '$.itemFeature.startTime') as recommend_time,
      |
      |--quality feature
      |get_json_object(msg, '$.itemFeature.cover_score') as cover_score,
      |get_json_object(msg, '$.itemFeature.effect_score') as effect_score,
      |get_json_object(msg, '$.itemFeature.rare_score') as rare_score,
      |get_json_object(msg, '$.itemFeature.time_score') as time_score,
      |
      |--reply_cnt
      |get_json_object(msg, '$.itemFeature.reply_cnt_1d') as reply_cnt_1d,
      |get_json_object(msg, '$.itemFeature.reply_cnt_3d') as reply_cnt_3d,
      |get_json_object(msg, '$.itemFeature.reply_cnt_7d') as reply_cnt_7d,
      |get_json_object(msg, '$.itemFeature.reply_cnt_15d') as reply_cnt_15d,
      |get_json_object(msg, '$.itemFeature.reply_cnt_30d') as reply_cnt_30d,
      |
      |
      |--deal_click_dur_log
      |get_json_object(msg, '$.userFeature.click_num_15d') as user_click_num_15d,
      |get_json_object(msg, '$.userFeature.click_num_30d') as user_click_num_30d,
      |get_json_object(msg, '$.userFeature.click_num_60d') as user_click_num_60d,
      |get_json_object(msg, '$.userFeature.click_num_7d') as user_click_num_7d,
      |get_json_object(msg, '$.userFeature.click_num_90d') as user_click_num_90d,
      |get_json_object(msg, '$.userFeature.duration_15d') as user_duration_15d,
      |get_json_object(msg, '$.userFeature.duration_30d') as user_duration_30d,
      |get_json_object(msg, '$.userFeature.duration_60d') as user_duration_60d,
      |get_json_object(msg, '$.userFeature.duration_7d') as user_duration_7d,
      |get_json_object(msg, '$.userFeature.duration_90d') as user_duration_90d,
      |
      |
      |
      |--deal_matcher
      |get_json_object(msg, '$.userFeature.series_list_7d') as item_series_list_7d,
      |get_json_object(msg, '$.userFeature.series_list_15d') as item_series_list_15d,
      |get_json_object(msg, '$.userFeature.series_list_30d') as item_series_list_30d,
      |
      |
      |
      |--新增类别特征
      |get_json_object(msg, '$.userFeature.uniq_keywords_pref') as user_uniq_keywords_pref,
      |get_json_object(msg, '$.userFeature.uniq_series_pref') as user_uniq_series_pref,
      |get_json_object(msg, '$.userFeature.fp_click_series_seq') as user_fp_click_series_seq,
      |get_json_object(msg, '$.itemFeature.uniq_series_ids') as item_uniq_series_ids,
      |get_json_object(msg, '$.itemFeature.uniq_keywords_name') as item_uniq_keywords_name,
      |get_json_object(msg, '$.userFeature.rt_fp_click_series_seq') as user_rt_fp_click_series_seq,
      |get_json_object(msg, '$.userFeature.uniq_category_pref') as user_uniq_category_pref,
      |get_json_object(msg, '$.itemFeature.uniq_category_name') as item_uniq_category_name,
      |get_json_object(msg, '$.userFeature.rt_category_list') as user_rt_category_list,
      |get_json_object(msg, '$.itemFeature.author_id') as item_author_id,
      |get_json_object(msg, '$.userFeature.rt_click_author_list_pre') as user_rt_click_author_list_pre,
      |get_json_object(msg, '$.userFeature.rt_click_tag_pref') as user_rt_click_tag_pref,
      |get_json_object(msg, '$.userFeature.device_model') as user_device_model,
      |get_json_object(msg, '$.userFeature.energy_pref') as user_energy_pref_top1,
      |get_json_object(msg, '$.itemFeature.recall_way') as recall_way,
      |get_json_object(msg, '$.itemFeature.gc_type') as gc_type,
      |get_json_object(msg, '$.itemFeature.rt_item_lst_list') as rt_item_lst_list,
      |get_json_object(msg, '$.itemFeature.item_lst_list') as item_lst_list,
      |concat(biz_type,'-',biz_id) as item_key,
      |
      |
      |label,
      |dt
      |from dm_rca.dm_rca_train_sample_all_shucang_v2_filter_by_user
      |where dt<='currDate' and dt >='preDate'
      |and biz_type > 0 and biz_id > 0 and device_id is not null and biz_type in ('3','14','66')
      |""".stripMargin

}
