package com.autohome.beans;

import lombok.Data;

import java.io.Serializable;

/**
 * @ClassName SourceBean
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/05 19:01
 **/
@Data
public class SourceBean implements Serializable {
    private String item_key;
    private String title;
    private String stitle;
    private String author;
    private String content;
    private long timestamp;
    private String topic;
    private String biz_type;
    private String biz_id;
    private String searchUsed;
    private String jsonReserve;

}
