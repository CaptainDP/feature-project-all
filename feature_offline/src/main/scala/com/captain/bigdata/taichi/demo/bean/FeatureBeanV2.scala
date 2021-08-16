package com.captain.bigdata.taichi.demo.bean


case class FeatureBeanV2(var device_id: String,
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

                         //结果字段
                         var item_callback_ratio_1d: String,
                         var item_callback_ratio_3d: String,
                         var item_callback_ratio_7d: String,
                         var item_callback_ratio_15d: String,
                         var item_callback_ratio_30d: String,

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

                         //collect_uv结果
                         var item_collect_uv_1: String,
                         var item_collect_uv_3: String,
                         var item_collect_uv_7: String,
                         var item_collect_uv_15: String,
                         var item_collect_uv_30: String,

                         //frontpage agee ctr
                         var item_frontpage_agee_pv_ctr_1d: String,
                         var item_frontpage_agee_pv_ctr_3d: String,
                         var item_frontpage_agee_pv_ctr_7d: String,
                         var item_frontpage_agee_pv_ctr_15d: String,
                         var item_frontpage_agee_pv_ctr_30d: String,

                         //click_pv: String,
                         var item_click_pv_wilson_1_3d: String,
                         var item_click_pv_wilson_1_7d: String,
                         var item_click_pv_wilson_1_15d: String,
                         var item_click_pv_wilson_1_30d: String,
                         var item_click_pv_wilson_3_7d: String,
                         var item_click_pv_wilson_3_15d: String,
                         var item_click_pv_wilson_3_30d: String,

                         //sight_pv: String,
                         var item_sight_pv_wilson_1_3d: String,
                         var item_sight_pv_wilson_1_7d: String,
                         var item_sight_pv_wilson_1_15d: String,
                         var item_sight_pv_wilson_1_30d: String,
                         var item_sight_pv_wilson_3_7d: String,
                         var item_sight_pv_wilson_3_15d: String,
                         var item_sight_pv_wilson_3_30d: String,


                         //collect_uv结果: String,
                         var item_collect_uv_1d: String,
                         var item_collect_uv_3d: String,
                         var item_collect_uv_7d: String,
                         var item_collect_uv_15d: String,
                         var item_collect_uv_30d: String,

                         //车系交叉特征
                         var item_series_list_7d: String,
                         var item_series_list_15d: String,
                         var item_series_list_30d: String,

                         //车系交叉匹配特征结果字段
                         var match_series_pv_maxmin_7d: String,
                         var match_series_dur_maxmin_7d: String,
                         var match_series_pv_weight_7d: String,
                         var match_series_dur_weight_7d: String,
                         var match_series_pv_7d: String,
                         var match_series_dur_7d: String,
                         var match_series_pv_maxmin_15d: String,
                         var match_series_dur_maxmin_15d: String,
                         var match_series_pv_weight_15d: String,
                         var match_series_dur_weight_15d: String,
                         var match_series_pv_15d: String,
                         var match_series_dur_15d: String,
                         var match_series_pv_maxmin_30d: String,
                         var match_series_dur_maxmin_30d: String,
                         var match_series_pv_weight_30d: String,
                         var match_series_dur_weight_30d: String,
                         var match_series_pv_30d: String,
                         var match_series_dur_30d: String,


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
                         var dt: String)


