package com.yonyou.dao.impl;

import com.yonyou.dao.FlowStatDAO;
import com.yonyou.entity.FlowStat;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.model.PVStatQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenxiaolei on 16/12/29.
 */
public class FlowStatDAOImpl implements FlowStatDAO{
    @Override
    public void updataBatch(List<FlowStat> flowStats, Connection conn) {
        List<FlowStat> insertFlowStats = new ArrayList<FlowStat>();
        List<FlowStat> updateFlowStats = new ArrayList<FlowStat>();
        String selectSQL = "SELECT count(*) "
                + "FROM rp_flow_count "
                + "WHERE created=? "
                + "WHERE siteType=? "
                + "AND type=? ";
        for (FlowStat flowStat : flowStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
            Object[] params = new Object[]{
                    flowStat.getCreated(),flowStat.getSiteType(),flowStat.getType()
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
                updateFlowStats.add(flowStat);
            } else {
                insertFlowStats.add(flowStat);
            }
        }
        //666666666666666666666666666666666666666666666666666
        // 执行插入操作 3个参数
        String insertSQL = "INSERT INTO rp_flow_count(type,siteType,created,flow) VALUES(?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<Object[]>();
        for (FlowStat flowStat :insertFlowStats){
            Object[] param =  new Object[]{
                    flowStat.getType(),flowStat.getSiteType(),flowStat.getCreated(),flowStat.getFlow()
            };
            insertParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,insertSQL, insertParamsList);
        //执行更新操作 需要更新数据 做查询
        String updateSQL = "UPDATE rp_flow_count set "
                + "flow = ? "
                + "WHERE created=? "
                + "WHERE siteType=? "
                + "AND type=? ";
        List<Object[]> updateParamsList = new ArrayList<Object[]>();
        for (FlowStat flowStat:updateFlowStats){
            Object[] param =  new Object[]{
                    flowStat.getFlow(),flowStat.getCreated(),flowStat.getSiteType(),flowStat.getType()
            };
            updateParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,updateSQL, updateParamsList);
    }
}
