package com.yonyou.dao;

import com.yonyou.entity.IPVStat;
import com.yonyou.entity.MemIDStat;

import java.sql.Connection;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/17.
 */
public interface IIPVStatDAO {
    void updataBatch(List<IPVStat> ipvStats, Connection conn);
}
