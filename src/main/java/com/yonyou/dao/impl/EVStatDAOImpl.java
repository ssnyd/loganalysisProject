package com.yonyou.dao.impl;

import com.yonyou.dao.IEVStatDAO;
import com.yonyou.entity.enterprise.EVStat;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.model.PVStatQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenxiaolei on 16/12/13.
 */
public class EVStatDAOImpl implements IEVStatDAO{
    @Override
    public void updataBatch(List<EVStat> evStats, Connection conn) {
        List<EVStat> insertEVStats = new ArrayList<EVStat>();
        List<EVStat> updateEVStats = new ArrayList<EVStat>();
        String selectSQL = "SELECT count(*) "
                + "FROM rp_ev_num "
                + "WHERE created=? "
                + "AND type=? ";
        for (EVStat evStat : evStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
            Object[] params = new Object[]{
                    evStat.getCreated(),evStat.getType()
            };
            JDBCHelper.executeQuery(conn,selectSQL,params,new JDBCHelper.QueryCallback(){
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()){
                        int count = rs.getInt(1);
                        queryResult.setCount(count);
                    }
                }
            });
            int count = queryResult.getCount();
            if(count > 0) {
                updateEVStats.add(evStat);
            } else {
                insertEVStats.add(evStat);
            }
        }
        //666666666666666666666666666666666666666666666666666
        // 执行插入操作 3个参数
        String insertSQL = "INSERT INTO rp_ev_num(type,created,num) VALUES(?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<Object[]>();
        for (EVStat evStat :insertEVStats){
            Object[] param =  new Object[]{
                    evStat.getType(),evStat.getCreated(),evStat.getNum()
            };
            insertParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,insertSQL, insertParamsList);
        //执行更新操作 需要更新数据 做查询
        String updateSQL = "UPDATE rp_ev_num set "
                + "num = ? "
                + "WHERE created=? "
                + "AND type=? ";
        List<Object[]> updateParamsList = new ArrayList<Object[]>();
        for (EVStat evStat:updateEVStats){
            Object[] param =  new Object[]{
                    evStat.getNum(),evStat.getCreated(),evStat.getType()
            };
            updateParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,updateSQL, updateParamsList);
    }
}
