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
public class FlowStatDAOImpl implements FlowStatDAO {
    @Override
    public void updataBatch(List<FlowStat> flowStats, Connection conn, int num) {
        List<FlowStat> insertFlowStats = new ArrayList<FlowStat>();
        List<FlowStat> updateFlowStats = new ArrayList<FlowStat>();
        String selectSQL = "SELECT count(*) "
                + "FROM rp_site_count "
                + "WHERE created=? "
                + "AND siteType=? "
                + "AND type=? ";
        for (FlowStat flowStat : flowStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
            Object[] params = new Object[]{
                    flowStat.getCreated(), flowStat.getSiteType(), flowStat.getType()
            };
            JDBCHelper.executeQuery(conn, selectSQL, params, new JDBCHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        queryResult.setCount(count);
                    }
                }
            });
            int count = queryResult.getCount();
            if (count > 0) {
                updateFlowStats.add(flowStat);
            } else {
                insertFlowStats.add(flowStat);
            }
        }
        String insertSQL = "";
        String updateSQL = "";
        if (num == 0) {//uv
            insertSQL = "INSERT INTO rp_site_count(type,siteType,created,uv) VALUES(?,?,?,?)";
            updateSQL = "UPDATE rp_site_count set "
                    + "uv = ? "
                    + "WHERE created=? "
                    + "AND siteType=? "
                    + "AND type=? ";
        } else if (num == 1) {//pv
            insertSQL = "INSERT INTO rp_site_count(type,siteType,created,pv) VALUES(?,?,?,?)";
            updateSQL = "UPDATE rp_site_count set "
                    + "pv = ? "
                    + "WHERE created=? "
                    + "AND siteType=? "
                    + "AND type=? ";
        } else if (num == 2) {//ipv
            insertSQL = "INSERT INTO rp_site_count(type,siteType,created,ipv) VALUES(?,?,?,?)";
            updateSQL = "UPDATE rp_site_count set "
                    + "ipv = ? "
                    + "WHERE created=? "
                    + "AND siteType=? "
                    + "AND type=? ";

        } else if (num == 3) {//traffic
            insertSQL = "INSERT INTO rp_site_count(type,siteType,created,traffic) VALUES(?,?,?,?)";
            updateSQL = "UPDATE rp_site_count set "
                    + "traffic = ? "
                    + "WHERE created=? "
                    + "AND siteType=? "
                    + "AND type=? ";

        }
        //666666666666666666666666666666666666666666666666666
        List<Object[]> insertParamsList = new ArrayList<Object[]>();
        for (FlowStat flowStat : insertFlowStats) {
            Object[] param = new Object[]{
                    flowStat.getType(), flowStat.getSiteType(), flowStat.getCreated(), flowStat.getFlow()
            };
            insertParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn, insertSQL, insertParamsList);

        List<Object[]> updateParamsList = new ArrayList<Object[]>();
        for (FlowStat flowStat : updateFlowStats) {
            Object[] param = new Object[]{
                    flowStat.getFlow(), flowStat.getCreated(), flowStat.getSiteType(), flowStat.getType()
            };
            updateParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn, updateSQL, updateParamsList);
    }
}
