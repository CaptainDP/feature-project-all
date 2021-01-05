package com.autohome.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.autohome.beans.Segement;
import com.autohome.models.OffsetModel;
import okhttp3.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @ClassName ESHttpClientUtils
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/04 18:07
 **/

public class ESHttpClientUtils {

    static String jieba_so_cluster_url = "http://10.28.237.64/_analyze";
    private static final MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
    private final static OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(30,TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.SECONDS)
            .writeTimeout(5, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true).build();

    /**
     * 请求es分词接口
     * @param content
     * @return
     */
    public static List<Segement> post(String content){

        String req = String.format("{\"analyzer\": \"jieba_mix_word\",\"text\": [\"%s\"]}", content);
        RequestBody body = RequestBody.create(mediaType, req);

        Request build = new Request.Builder()
                .url(jieba_so_cluster_url)
                .post(body)
                .build();

        try {
            Response execute = client.newCall(build).execute();
            String bodyContent = execute.body().string();

            JSONObject jsonObject = JSONObject.parseObject(bodyContent);
            JSONArray tokens = jsonObject.getJSONArray("tokens");

            List<Segement> collect = tokens.stream().map(x -> {
                JSONObject item = (JSONObject) x;
                Segement seg = new Segement();
                seg.setType(item.getString("type"));
                seg.setEnd_offset(item.getLong("end_offset"));
                seg.setPosition(item.getLong("position"));
                seg.setToken(item.getString("token"));
                seg.setStart_offset(item.getLong("start_offset"));
                return seg;
            }).limit(200).collect(Collectors.toList());

            return collect;
        } catch (Exception e) {
            return null;
        }
    }


    /**
     * 偏移量转pb
     * @param title
     * @param stitle
     * @param author
     * @param content
     * @return
     */
    public static OffsetModel.Offset castPb(List<Segement> title, List<Segement> stitle, List<Segement> author, List<Segement> content){
        OffsetModel.Offset.Builder offset = OffsetModel.Offset.newBuilder();

        title.stream().map(x -> offset.putTitleTermList(x.getToken(), (int) x.getStart_offset()));
        stitle.stream().map(x -> offset.putStitleTermList(x.getToken(), (int) x.getStart_offset()));
        author.stream().map(x -> offset.putAuthorTermList(x.getToken(), (int) x.getStart_offset()));
        content.stream().map(x -> offset.putContentTermList(x.getToken(), (int) x.getStart_offset()));

        return offset.build();

    }
}
