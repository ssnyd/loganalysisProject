package com.yonyou.entity.enterprise;

import java.io.Serializable;

/**
 * Created by chenxiaolei on 16/12/14.
 */
public class EUPV implements Serializable{
    private String type;
    private String instanceId;
    private String created;
    private Integer euvNum;
    private Integer epvNum;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public Integer getEuvNum() {
        return euvNum;
    }

    public void setEuvNum(Integer euvNum) {
        this.euvNum = euvNum;
    }

    public Integer getEpvNum() {
        return epvNum;
    }

    public void setEpvNum(Integer epvNum) {
        this.epvNum = epvNum;
    }
}
