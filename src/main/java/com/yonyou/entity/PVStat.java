package com.yonyou.entity;

import java.io.Serializable;

/**
 * Created by ChenXiaoLei on 2016/11/9.
 */
public class PVStat implements Serializable{
    private static final long serialVersionUID = 2985756100635634686L;
    private String timestamp;
    private String region;
    private int ClickCount;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public int getClickCount() {
        return ClickCount;
    }

    public void setClickCount(int clickCount) {
        ClickCount = clickCount;
    }
}
