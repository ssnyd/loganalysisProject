package com.yonyou.entity;

import java.io.Serializable;

/**
 * Created by ChenXiaoLei on 2016/11/1.
 */
public class Request implements Serializable {
    private String request_mode;
    private String Http_mode;
    private String body;

    public String getRequest_mode() {
        return request_mode;
    }

    public void setRequest_mode(String request_mode) {
        this.request_mode = request_mode;
    }

    public String getHttp_mode() {
        return Http_mode;
    }

    public void setHttp_mode(String http_mode) {
        Http_mode = http_mode;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}
