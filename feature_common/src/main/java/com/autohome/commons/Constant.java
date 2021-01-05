package com.autohome.commons;

/**
 * @ClassName Constant
 * @Description TODO
 * @Author chenhc
 * @Date 2020/08/16 16:45
 **/

public class Constant {
    /*prod redis*/
    public static final String REDIS_HOST = "c_4940h368.codis.lfpsg2.in.autohome.com.cn";
    public static final Integer REDIS_PORT = 32360;
    public static final String REDIS_PASSWD = "w_n70aNP4Ln0b0D6";
    public static final Integer REDIS_DB = 0;

    /* test redis*/
    /*public static final String REDIS_HOST = "c_yml0cn9v.codis.lfpsg1.in.autohome.com.cn";
    public static final Integer REDIS_PORT = 32030;
    public static final String REDIS_PASSWD = "_I3673Oq4O6l1zOq";
    public static final Integer REDIS_DB = 0;*/

    public static final String es_username = "index_elastic";
    public static final String es_passwd= "indexserviceprovider";

    /*elasticsearch*/
    public static final String ES_HOST = "10.27.4.226";
    public static final Integer ES_PORT = 9201;
    public static final String ES_INDEX = "search_topic_1";
    public static final String ES_TYPE = "_doc";

    /*mysql*/
    public static final String DRIVER = "com.mysql.jdbc.Driver";
    public static final String BDP_URL = "jdbc:mysql://dp-mw0-3306-db.lq.autohome.com.cn/bdp_recommend_operate";
    public static final String BDP_USERNAME= "bdp_recommend_operate_read";
    public static final String BDP_PASSWD = "BvO4dAOsZmdxjgQH";

    public static final String RESOURCE_URL = "jdbc:mysql://zyc_read_tidb-sr0-3306s.tidb.db.corpautohome.com/resource_pool_merge";
    public static final String RESOURCE_USERNAME= "recall_r";
    public static final String RESOURCE_PASSWD = "2vsv2YTXQJnFuJhY";

    public static final String TITLE = "title";
    public static final String LTITLE = "long_title";
    public static final String CONTENT = "content";
    public static final String AUTHOR = "author";
    public static final String BBSNAME = "bbs_name";

    public static final String REDIS_KEY_PREFIX = "offset_";

    public static final Integer BATCH_EXECUTE_SIZE = 1000000;
}
