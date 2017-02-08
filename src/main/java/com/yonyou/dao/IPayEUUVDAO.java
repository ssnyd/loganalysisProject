package com.yonyou.dao;

import com.yonyou.entity.enterprise.EVStat;

import java.sql.Connection;
import java.util.List;

/**
 * Created by chenxiaolei on 17/1/23.
 */
public interface IPayEUUVDAO {
    /**
     * 查询所有付费企业 id
     * @return
     */
    List<String> findAll(Connection conn,String start,String end);
    void updataBatch(List<EVStat> evStats, Connection conn,int flag);


    //updataBatch
}
