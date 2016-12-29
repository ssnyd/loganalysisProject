package com.yonyou.dao;

import com.yonyou.entity.applyStat;

import java.sql.Connection;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/27.
 */
public interface IApplyQStatDAO {
    void updataBatch(List<applyStat> applyStats, Connection conn, Integer num);
}
