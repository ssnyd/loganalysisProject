package com.yonyou.entity;

import java.io.Serializable;

/**
 * Created by ChenXiaoLei on 2016/11/27.
 */
public class applyStat implements Serializable {
    private static final long serialVersionUID = -8591250327149763909L;
    private String created;
    private String rpid;
    private String action;
    private String myType;
    public String getMyType() {
        return myType;
    }

    public void setMyType(String myType) {
        this.myType = myType;
    }
    private Integer category;

    public Integer getCategory() {
        return category;
    }

    public void setCategory(Integer category) {
        this.category = category;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getRpid() {
        return rpid;
    }

    public void setRpid(String rpid) {
        this.rpid = rpid;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
