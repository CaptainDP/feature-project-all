package com.autohome.maps;

import com.alibaba.fastjson.JSONObject;
import com.autohome.beans.Segement;
import com.autohome.beans.SourceBean;
import com.autohome.models.OffsetModel;
import com.autohome.utils.ESHttpClientUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.List;

/**
 * @ClassName PbHandlerMap
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/05 20:25
 **/

public class PbHandlerMap extends RichMapFunction<SourceBean, JSONObject> {
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
}

