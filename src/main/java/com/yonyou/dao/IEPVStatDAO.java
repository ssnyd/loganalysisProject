package com.yonyou.dao;

import com.yonyou.entity.enterprise.EUPV;

import java.sql.Connection;
import java.util.List;

/**
 * Created by chenxiaolei on 16/12/14.
 */
public interface IEPVStatDAO {
    void updataBatch(List<EUPV> eupvStats, Connection conn);

}
