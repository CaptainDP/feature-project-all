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

    public String getItem_key() {
        return item_key;
    }

    public void setItem_key(String item_key) {
        this.item_key = item_key;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getStitle() {
        return stitle;
    }

    public void setStitle(String stitle) {
        this.stitle = stitle;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBiz_type() {
        return biz_type;
    }

    public void setBiz_type(String biz_type) {
        this.biz_type = biz_type;
    }

    public String getBiz_id() {
        return biz_id;
    }

    public void setBiz_id(String biz_id) {
        this.biz_id = biz_id;
    }

    public String getSearchUsed() {
        return searchUsed;
    }

    public void setSearchUsed(String searchUsed) {
        this.searchUsed = searchUsed;
    }

    public String getJsonReserve() {
        return jsonReserve;
    }

    public void setJsonReserve(String jsonReserve) {
        this.jsonReserve = jsonReserve;
    }
}
