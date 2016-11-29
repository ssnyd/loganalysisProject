package com.yonyou.dao.impl;

import com.yonyou.dao.IApplyStatDAO;
import com.yonyou.entity.applyStat;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.model.PVStatQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/27.
 */
public class ApplyStatImpl implements IApplyStatDAO {
    @Override
    public void updataBatch(List<applyStat> applyStats, Connection conn,Integer num) {
        List<applyStat> insertApplyStats = new ArrayList<applyStat>();
        List<applyStat> updateApplyStats = new ArrayList<applyStat>();
        String selectSQL = "SELECT count(*) "
                + "FROM rp_app_action_num "
                + "WHERE created=? "
                + "AND rpAppId=? "
                + "AND action=? ";
        for (applyStat applystat : applyStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
            Object[] params = new Object[]{
                    applystat.getCreated(),applystat.getRpid(),applystat.getAction()
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
                updateApplyStats.add(applystat);
            } else {
                insertApplyStats.add(applystat);
            }
        }
         String insertSQL = "";
        String updateSQL = "";
        // 执行插入操作 4个参数
        if (num == 0){
            insertSQL = "INSERT INTO rp_app_action_num(created,rpAppId,action,enterpriseNum) VALUES(?,?,?,?)";
            updateSQL = "UPDATE rp_app_action_num set "
                + "enterpriseNum=? "
                + "WHERE created=? "
                + "AND rpAppId=? "
                + "AND action=? ";
        }else if (num == 1){
            insertSQL = "INSERT INTO rp_app_action_num(created,rpAppId,action,qzNum) VALUES(?,?,?,?)";
            updateSQL = "UPDATE rp_app_action_num set "
                + "qzNum=? "
                + "WHERE created=? "
                + "AND rpAppId=? "
                + "AND action=? ";
        }else if (num == 2){
            insertSQL = "INSERT INTO rp_app_action_num(created,rpAppId,action,memberNum) VALUES(?,?,?,?)";
            updateSQL = "UPDATE rp_app_action_num set "
                + "memberNum=? "
                + "WHERE created=? "
                + "AND rpAppId=? "
                + "AND action=? ";
        }

        List<Object[]> insertParamsList = new ArrayList<Object[]>();
        for (applyStat applystat :insertApplyStats){
            Object[] param =  new Object[]{
                    applystat.getCreated(),applystat.getRpid(),applystat.getAction(),applystat.getCategory()
            };
            insertParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,insertSQL, insertParamsList);
        //执行更新操作 需要更新数据 做查询

        List<Object[]> updateParamsList = new ArrayList<Object[]>();
        for (applyStat applystat :updateApplyStats){
            Object[] param =  new Object[]{
                    applystat.getCategory(),applystat.getCreated(),applystat.getRpid(),applystat.getAction()
            };
            updateParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,updateSQL, updateParamsList);





















    }
}
