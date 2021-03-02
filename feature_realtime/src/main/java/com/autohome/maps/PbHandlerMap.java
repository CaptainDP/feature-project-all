package com.autohome.maps;

import com.alibaba.fastjson.JSONObject;
import com.autohome.beans.Segement;
import com.autohome.beans.SourceBean;
import com.autohome.models.OffsetModel;
import com.autohome.utils.HttpClientUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @ClassName PbHandlerMap
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/05 20:25
 **/

public class PbHandlerMap extends RichMapFunction<SourceBean, JSONObject> {
    private static Logger logger = LoggerFactory.getLogger("PbHandlerMap");

    String jieSoServerFlag;
    public PbHandlerMap(String jieSoServerFlag){
        this.jieSoServerFlag = jieSoServerFlag;
    }
    @Override
    public JSONObject map(SourceBean input) throws Exception {
        String author = input.getAuthor();
        String content = input.getContent();
        String stitle = input.getStitle();
        String title = input.getTitle();
        OffsetModel.Offset offset = null;
        if("es".equalsIgnoreCase(jieSoServerFlag))
            offset = queryES(author, content, stitle, title);
        else if("qp".equalsIgnoreCase(jieSoServerFlag))
            offset = queryQP(author, content, stitle, title);
        else
            throw new IllegalArgumentException("jieba so server jieSoServerFlag error，must be 【es,qp】");
        JSONObject output = new JSONObject().fluentPut("offset", offset)
                .fluentPut("item_key", input.getItem_key());
        return output;
    }
    /***
     * 查询QP分词接口
     * @param author
     * @param content
     * @param stitle
     * @param title
     * @return
     */
    public OffsetModel.Offset queryQP(String author,String content,String stitle,String title){
        List<Segement> authorSegList = HttpClientUtils.postQP(author);
        List<Segement> contentSegList = HttpClientUtils.postQP(content);
        List<Segement> stitleSegList = HttpClientUtils.postQP(stitle);
        List<Segement> titleSegList = HttpClientUtils.postQP(title);
        OffsetModel.Offset pb = HttpClientUtils.createPB(authorSegList, contentSegList, stitleSegList, titleSegList);
        return pb;
    }

    /***
     * 查询es分词接口
     * @param author
     * @param content
     * @param stitle
     * @param title
     * @return
     */
    public OffsetModel.Offset queryES(String author,String content,String stitle,String title){
        List<Segement> authorSegList = HttpClientUtils.postES(author);
        List<Segement> contentSegList = HttpClientUtils.postES(content);
        List<Segement> stitleSegList = HttpClientUtils.postES(stitle);
        List<Segement> titleSegList = HttpClientUtils.postES(title);
        OffsetModel.Offset pb = HttpClientUtils.createPB(authorSegList, contentSegList, stitleSegList, titleSegList);
        return pb;
    }
}

