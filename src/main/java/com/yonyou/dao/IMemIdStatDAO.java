package com.yonyou.dao;

import com.yonyou.entity.MemIDStat;

import java.sql.Connection;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/16.
 */
public interface IMemIdStatDAO {
    void updataBatch(List<MemIDStat> memIDStats, Connection connection);
}
