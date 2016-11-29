package com.yonyou.dao;

import com.yonyou.entity.UVStat;

import java.sql.Connection;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/16.
 */
public interface IUVStatDAO {
    void updataBatch(List<UVStat> uvStats, Connection connection);
}
