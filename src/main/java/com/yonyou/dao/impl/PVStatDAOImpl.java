package com.yonyou.dao.impl;

import com.yonyou.dao.IPVStatDAO;
import com.yonyou.entity.PVStat;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.model.PVStatQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ChenXiaoLei on 2016/11/17.
 */
public class PVStatDAOImpl implements IPVStatDAO {

    @Override
    public void updataBatch(List<PVStat> pvStats, Connection conn) {
        Map<String, HashMap<String, Integer>> insertPVStats = new HashMap<String, HashMap<String, Integer>>();
        HashMap<String, Integer> insert = new HashMap<String, Integer>();
        Map<String, HashMap<String, Integer>> updatePVStats = new HashMap<String, HashMap<String, Integer>>();
        HashMap<String, Integer> update = new HashMap<String, Integer>();
        //for (PVStat pvStat : pvStats) {
        //    value.put(pvStat.getRegion(), pvStat.getClickCount());
        //    map.put(pvStat.getTimestamp(), value);
        //}
        String selectSQL = "SELECT count(*) "
                + "FROM rp_pv_count "
                + "WHERE created=? "
                + "AND clientType=? ";
        for (PVStat pvStat : pvStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
            Object[] params = new Object[]{
                    pvStat.getTimestamp(),
                    "all"
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
            if (count > 0) {//更新操作
                update.put(pvStat.getRegion(), pvStat.getClickCount());
                updatePVStats.put(pvStat.getTimestamp(), update);

            } else {//插入操作
                update.put(pvStat.getRegion(), pvStat.getClickCount());
                updatePVStats.put(pvStat.getTimestamp(), update);

            }
        }
        //for (PVStat pvStat : pvStats) {
        //    final PVStatQueryResult queryResult = new PVStatQueryResult();
        //    Object[] params = new Object[]{
        //            pvStat.getTimestamp(),
        //            "all"
        //    };
        //    JDBCHelper.executeQuery(conn, selectSQL, params, new JDBCHelper.QueryCallback() {
        //        @Override
        //        public void process(ResultSet rs) throws Exception {
        //            if (rs.next()) {
        //                int count = rs.getInt(1);
        //                queryResult.setCount(count);
        //            }
        //        }
        //    });
        //    int count = queryResult.getCount();
        //    if (count > 0) {
        //        updatePVStats.add(pvStat);
        //    } else {
        //        insertPVStats.add(pvStat);
        //    }
        //}
        //
        //////********************************
        ////StringBuffer insertSQL = new StringBuffer("INSERT INTO rp_pv_count(created,clientType,");
        ////
        //////********************************
        ////for (PVStat pvStat:insertPVStats){
        ////}


    }
}
