package com.yonyou.dao;

import com.yonyou.entity.enterprise.EVStat;

import java.sql.Connection;
import java.util.List;

/**
 * Created by chenxiaolei on 16/12/13.
 */
public interface IEVStatDAO {
    void updataBatch(List<EVStat> evStats, Connection connection);
}
