package com.yonyou.entity;

import java.io.Serializable;

/**
 * Created by ChenXiaoLei on 2016/11/16.
 */
public class UVStat implements Serializable {
    private static final long serialVersionUID = 4929668760677360418L;
    private String type;
    private String clientType;
    private String created;
    private Integer num;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getClientType() {
        return clientType;
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }
}
