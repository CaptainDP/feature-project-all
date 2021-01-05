package com.autohome.beans;

/**
 * @ClassName Segement
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/04 18:16
 **/

public class Segement {
    private String token;
    private String type;
    private long start_offset;
    private long end_offset;
    private long position;


    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getStart_offset() {
        return start_offset;
    }

    public void setStart_offset(long start_offset) {
        this.start_offset = start_offset;
    }

    public long getEnd_offset() {
        return end_offset;
    }

    public void setEnd_offset(long end_offset) {
        this.end_offset = end_offset;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }
}
