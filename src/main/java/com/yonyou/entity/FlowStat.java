package com.yonyou.entity;

import java.io.Serializable;

/**
 * Created by chenxiaolei on 16/12/29.
 */
public class FlowStat implements Serializable{
    private static final long serialVersionUID = 2988656210742646946L;
    private String created;
    private String type;
    private String siteType;
    private double flow;

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSiteType() {
        return siteType;
    }

    public void setSiteType(String siteType) {
        this.siteType = siteType;
    }

    public double getFlow() {
        return flow;
    }

    public void setFlow(double flow) {
        this.flow = flow;
    }


}
