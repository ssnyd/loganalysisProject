package com.yonyou.dao;

import com.yonyou.entity.FlowStat;

import java.sql.Connection;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/16.
 */
public interface FlowStatDAO {
    void updataBatch(List<FlowStat> flowStat, Connection connection);
}
