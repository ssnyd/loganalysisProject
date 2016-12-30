package com.yonyou.entity.enterprise;

import java.io.Serializable;

/**
 * Created by chenxiaolei on 16/12/13.
 */
public class EVStat implements Serializable{
    private static final long serialVersionUID = -7315533188839279053L;
    private String type;
    private String created;
    private Integer num;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
