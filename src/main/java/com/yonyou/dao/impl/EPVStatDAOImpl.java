package com.yonyou.dao.impl;

import com.yonyou.dao.IEPVStatDAO;
import com.yonyou.entity.enterprise.EUPV;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.model.PVStatQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenxiaolei on 16/12/14.
 */
public class EPVStatDAOImpl implements IEPVStatDAO{
    @Override
    public void updataBatch(List<EUPV> eupvStats, Connection conn) {
        List<EUPV> insertEPVStats = new ArrayList<EUPV>();
        List<EUPV> updateEPVStats = new ArrayList<EUPV>();
        String selectSQL = "SELECT count(*) "
                + "FROM rp_enterprise_inner_access_num "
                + "WHERE created=? "
                + "AND instanceId=? "
                + "AND type=? ";
        for (EUPV eupv : eupvStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
            Object[] params = new Object[]{
                    eupv.getCreated(),eupv.getInstanceId(),eupv.getType()
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
                updateEPVStats.add(eupv);
            } else {
                insertEPVStats.add(eupv);
            }
        }
        //6666666666666666666666666666666666666
        // 执行插入操作 3个参数
        String insertSQL = "INSERT INTO rp_enterprise_inner_access_num(type,created,instanceId,pv) VALUES(?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<Object[]>();
        for (EUPV eupv :insertEPVStats){
            Object[] param =  new Object[]{
                    eupv.getType(),eupv.getCreated(),eupv.getInstanceId(),eupv.getEpvNum()
            };
            insertParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,insertSQL, insertParamsList);
        //执行更新操作 需要更新数据 做查询
        String updateSQL = "UPDATE rp_enterprise_inner_access_num set "
                + "pv = ? "
                + "WHERE created=? "
                + "AND instanceId=? "
                + "AND type=? ";
        List<Object[]> updateParamsList = new ArrayList<Object[]>();
        for (EUPV eupv : updateEPVStats){
            Object[] param =  new Object[]{
                    eupv.getEpvNum(),eupv.getCreated(),eupv.getInstanceId(),eupv.getType()
            };
            updateParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,updateSQL, updateParamsList);
    }
}
