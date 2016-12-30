package com.yonyou.entity;

import java.io.Serializable;

/**
 * Created by ChenXiaoLei on 2016/11/16.
 */
public class MemIDStat implements Serializable{
    private static final long serialVersionUID = 4980507775120932802L;
    private String created ;
    private String memberId ;
    private String qzId;

    public String getQzId() {
        return qzId;
    }

    public void setQzId(String qzId) {
        this.qzId = qzId;
    }

    private Integer times ;

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public Integer getTimes() {
        return times;
    }

    public void setTimes(Integer times) {
        this.times = times;
    }
}
