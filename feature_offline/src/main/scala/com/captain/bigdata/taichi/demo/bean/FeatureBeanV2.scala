package com.captain.bigdata.taichi.demo.bean


case class FeatureBeanV2(
                          //-----------------------------------库表字段-------------------------------------------------
                          //基础信息
                          var device_id: String,
                          var biz_type: String,
                          var biz_id: String,
                          var pvid: String,
                          var posid: String,

                          //点击类
                          var pv_1d: String,
                          var pv_3d: String,
                          var pv_7d: String,
                          var pv_15d: String,
                          var pv_30d: String,
                          var uv_1d: String,
                          var uv_3d: String,
                          var uv_7d: String,
                          var uv_15d: String,
                          var uv_30d: String,

                          //click_pv
                          var click_pv_1d: String,
                          var click_pv_3d: String,
                          var click_pv_7d: String,
                          var click_pv_15d: String,
                          var click_pv_30d: String,

                          //sight_pv
                          var sight_pv_1d: String,
                          var sight_pv_3d: String,
                          var sight_pv_7d: String,
                          var sight_pv_15d: String,
                          var sight_pv_30d: String,

                          //collect_cnt
                          var collect_cnt_1d: String,
                          var collect_cnt_3d: String,
                          var collect_cnt_7d: String,
                          var collect_cnt_15d: String,
                          var collect_cnt_30d: String,

                          //click_uv
                          var click_uv_1d: String,
                          var click_uv_3d: String,
                          var click_uv_7d: String,
                          var click_uv_15d: String,
                          var click_uv_30d: String,

                          //车系交叉特征
                          var item_series_list_7d: String,
                          var item_series_list_15d: String,
                          var item_series_list_30d: String,

                          //time
                          var start_time: String,
                          var recommend_time: String,

                          //quality feature
                          var cover_score: String,
                          var effect_score: String,
                          var rare_score: String,
                          var time_score: String,

                          //reply_cnt
                          var reply_cnt_1d: String,
                          var reply_cnt_3d: String,
                          var reply_cnt_7d: String,
                          var reply_cnt_15d: String,
                          var reply_cnt_30d: String,

                          //like
                          var like_cnt_1d: String,
                          var like_cnt_3d: String,
                          var like_cnt_7d: String,
                          var like_cnt_15d: String,
                          var like_cnt_30d: String,

                          //
                          fans_num: String,
                          works_num: String,
                          comment_cnt_90d: String,
                          favorites_cnt_90d: String,
                          share_cnt_90d: String,
                          like_cnt_90d: String,

                          //click duration
                          var user_click_num_15d: String,
                          var user_click_num_30d: String,
                          var user_click_num_60d: String,
                          var user_click_num_7d: String,
                          var user_click_num_90d: String,
                          var user_duration_15d: String,
                          var user_duration_30d: String,
                          var user_duration_60d: String,
                          var user_duration_7d: String,
                          var user_duration_90d: String,

                          //类别特征
                          var user_uniq_keywords_pref: String,
                          var user_uniq_series_pref: String,
                          var user_fp_click_series_seq: String,
                          var item_uniq_series_ids: String,
                          var item_uniq_keywords_name: String,
                          var user_rt_fp_click_series_seq: String,
                          var user_uniq_category_pref: String,
                          var item_uniq_category_name: String,
                          var user_rt_category_list: String,
                          var item_author_id: String,
                          var user_rt_click_author_list_pre: String,
                          var user_rt_click_tag_pref: String,
                          var user_device_model: String,
                          var user_energy_pref_top1: String,
                          var recall_way: String,
                          var gc_type: String,
                          var rt_item_lst_list: String,
                          var item_lst_list: String,
                          var item_key: String,
                          var label: String,
                          var dt: String

                        ) {


  //--------------------------------------------------------结果字段----------------------------------------------------
  //pv-uv
  var item_callback_ratio_1d: String = ""
  var item_callback_ratio_3d: String = ""
  var item_callback_ratio_7d: String = ""
  var item_callback_ratio_15d: String = ""
  var item_callback_ratio_30d: String = ""

  //collect_uv结果
  var item_collect_uv_1: String = ""
  var item_collect_uv_3: String = ""
  var item_collect_uv_7: String = ""
  var item_collect_uv_15: String = ""
  var item_collect_uv_30: String = ""


  //frontpage agee ctr
  var item_frontpage_agee_pv_ctr_1d: String = ""
  var item_frontpage_agee_pv_ctr_3d: String = ""
  var item_frontpage_agee_pv_ctr_7d: String = ""
  var item_frontpage_agee_pv_ctr_15d: String = ""
  var item_frontpage_agee_pv_ctr_30d: String = ""

  //frontpage_click_pv
  var item_frontpage_click_pv_1d: String = ""
  var item_frontpage_click_pv_3d: String = ""
  var item_frontpage_click_pv_7d: String = ""
  var item_frontpage_click_pv_15d: String = ""
  var item_frontpage_click_pv_30d: String = ""

  //frontpage_sight_pv
  var item_frontpage_sight_pv_1d: String = ""
  var item_frontpage_sight_pv_3d: String = ""
  var item_frontpage_sight_pv_7d: String = ""
  var item_frontpage_sight_pv_15d: String = ""
  var item_frontpage_sight_pv_30d: String = ""

  //prop_author
  var item_prop_author_fellowsnum: String = ""
  var item_prop_author_articlenum: String = ""
  var item_prop_author_comment_num: String = ""
  var item_prop_author_favorites_num: String = ""
  var item_prop_author_share_cnt: String = ""
  var item_prop_author_like_num: String = ""


  var item_prop_publishtime: String = ""
  var item_prop_time_decay: String = ""

  //quality feature结果
  var item_quality_cover_score: String = ""
  var item_quality_effect_score: String = ""
  var item_quality_rare_score: String = ""
  var item_quality_time_score: String = ""

  //reply ratio
  var item_reply_uv_1d: String = ""
  var item_reply_uv_3d: String = ""
  var item_reply_uv_7d: String = ""
  var item_reply_uv_15d: String = ""
  var item_reply_uv_30d: String = ""

  //like ratio
  var item_like_uv_1d: String = ""
  var item_like_uv_3d: String = ""
  var item_like_uv_7d: String = ""
  var item_like_uv_15d: String = ""
  var item_like_uv_30d: String = ""

  //frontpage ctr
  var item_frontpage_wilson_pv_ctr_1d: String = ""
  var item_frontpage_wilson_pv_ctr_3d: String = ""
  var item_frontpage_wilson_pv_ctr_7d: String = ""
  var item_frontpage_wilson_pv_ctr_15d: String = ""
  var item_frontpage_wilson_pv_ctr_30d: String = ""

  //click_pv: String = ""
  var item_click_pv_wilson_1_3d: String = ""
  var item_click_pv_wilson_1_7d: String = ""
  var item_click_pv_wilson_1_15d: String = ""
  var item_click_pv_wilson_1_30d: String = ""
  var item_click_pv_wilson_3_7d: String = ""
  var item_click_pv_wilson_3_15d: String = ""
  var item_click_pv_wilson_3_30d: String = ""

  //sight_pv: String = ""
  var item_sight_pv_wilson_1_3d: String = ""
  var item_sight_pv_wilson_1_7d: String = ""
  var item_sight_pv_wilson_1_15d: String = ""
  var item_sight_pv_wilson_1_30d: String = ""
  var item_sight_pv_wilson_3_7d: String = ""
  var item_sight_pv_wilson_3_15d: String = ""
  var item_sight_pv_wilson_3_30d: String = ""


  //collect_uv结果: String = ""
  var item_collect_uv_1d: String = ""
  var item_collect_uv_3d: String = ""
  var item_collect_uv_7d: String = ""
  var item_collect_uv_15d: String = ""
  var item_collect_uv_30d: String = ""


  //车系交叉匹配特征结果字段
  var match_series_pv_maxmin_7d: String = ""
  var match_series_dur_maxmin_7d: String = ""
  var match_series_pv_weight_7d: String = ""
  var match_series_dur_weight_7d: String = ""
  var match_series_pv_7d: String = ""
  var match_series_dur_7d: String = ""
  var match_series_pv_maxmin_15d: String = ""
  var match_series_dur_maxmin_15d: String = ""
  var match_series_pv_weight_15d: String = ""
  var match_series_dur_weight_15d: String = ""
  var match_series_pv_15d: String = ""
  var match_series_dur_15d: String = ""
  var match_series_pv_maxmin_30d: String = ""
  var match_series_dur_maxmin_30d: String = ""
  var match_series_pv_weight_30d: String = ""
  var match_series_dur_weight_30d: String = ""
  var match_series_pv_30d: String = ""
  var match_series_dur_30d: String = ""

}

