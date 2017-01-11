package com.yonyou.dao;

import com.yonyou.entity.PVStat;

import java.sql.Connection;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/17.
 */
public interface IPVStatDAO {
    void updataBatch(List<PVStat> pvStats, Connection conn);
}
