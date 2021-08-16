package com.captain.bigdata.taichi.demo.output

object VideoFeatureColumnV2Output {

  val featuresListOutputReal = Array(

    //base
    "biz_id",
    "biz_type",
    "pvid",
    "device_id",
    "posid",

    //pv-uv
    "item_callback_ratio_1d",
    "item_callback_ratio_3d",
    "item_callback_ratio_7d",
    "item_callback_ratio_15d",
    "item_callback_ratio_30d",

    //frontpage agee ctr
    "item_frontpage_agee_pv_ctr_1d",
    "item_frontpage_agee_pv_ctr_3d",
    "item_frontpage_agee_pv_ctr_7d",
    "item_frontpage_agee_pv_ctr_15d",
    "item_frontpage_agee_pv_ctr_30d",


    //deal_click_dur_log
    "user_click_num_15d",
    "user_click_num_30d",
    "user_click_num_60d",
    "user_click_num_7d",
    "user_click_num_90d",
    "user_duration_15d",
    "user_duration_30d",
    "user_duration_60d",
    "user_duration_7d",
    "user_duration_90d",

    //车系交叉匹配特征结果字段
    "match_series_pv_maxmin_7d",
    "match_series_dur_maxmin_7d",
    "match_series_pv_weight_7d",
    "match_series_dur_weight_7d",
    "match_series_pv_7d",
    "match_series_dur_7d",
    "match_series_pv_maxmin_15d",
    "match_series_dur_maxmin_15d",
    "match_series_pv_weight_15d",
    "match_series_dur_weight_15d",
    "match_series_pv_15d",
    "match_series_dur_15d",
    "match_series_pv_maxmin_30d",
    "match_series_dur_maxmin_30d",
    "match_series_pv_weight_30d",
    "match_series_dur_weight_30d",
    "match_series_pv_30d",
    "match_series_dur_30d",


    //新增类别特征
    "user_uniq_keywords_pref",
    "user_uniq_series_pref",
    "user_fp_click_series_seq",
    "item_uniq_series_ids",
    "item_uniq_keywords_name",
    "user_rt_fp_click_series_seq",
    "user_uniq_category_pref",
    "item_uniq_category_name",
    "user_rt_category_list",
    "item_author_id",
    "user_rt_click_author_list_pre",
    "user_rt_click_tag_pref",
    "user_device_model",
    "user_energy_pref_top1",
    "recall_way",
    "gc_type",
    "biz_type",
    "rt_item_lst_list",
    "item_lst_list",
    "item_key",
    "label"
  )

}
