package com.yonyou.dao.impl;

import com.yonyou.dao.IIPVStatDAO;
import com.yonyou.entity.IPVStat;
import com.yonyou.entity.MemIDStat;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.model.PVStatQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/17.
 */
public class IPVStatDAOImpl implements IIPVStatDAO{
    @Override
    public void updataBatch(List<IPVStat> ipvStats, Connection conn) {
        List<IPVStat> insertIPVStats = new ArrayList<IPVStat>();
        List<IPVStat> updateIPVStats = new ArrayList<IPVStat>();
        String selectSQL = "SELECT count(*) "
                + "FROM rp_ipv_count "
                + "WHERE created=? "
                + "AND type=? "
                + "AND clientType=? ";
        for (IPVStat ipvStat : ipvStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
            Object[] params = new Object[]{
                    ipvStat.getCreated(),ipvStat.getType(),ipvStat.getClientType()
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
                updateIPVStats.add(ipvStat);
            } else {
                insertIPVStats.add(ipvStat);
            }
        }
        //6666666666666666666666666666666666666666666666666666666666666666666
        // 执行插入操作 3个参数
            String insertSQL = "INSERT INTO rp_ipv_count(type,clientType,created,num) VALUES(?,?,?,?)";
            List<Object[]> insertParamsList = new ArrayList<Object[]>();
            for (IPVStat ipvStat : insertIPVStats){
               Object[] param =  new Object[]{
                    ipvStat.getType(),ipvStat.getClientType(),ipvStat.getCreated(),ipvStat.getNum()
                };
                insertParamsList.add(param);
            }
            JDBCHelper.executeBatch(conn,insertSQL, insertParamsList);
        //执行更新操作 需要更新数据 做查询
        String updateSQL = "UPDATE rp_ipv_count set "
                + "num = ? "
                 + "WHERE created=? "
                 + "AND type=? "
                + "AND clientType=? ";
        List<Object[]> updateParamsList = new ArrayList<Object[]>();
        for (IPVStat ipvStat :updateIPVStats){
            Object[] param =  new Object[]{
                    ipvStat.getNum(),ipvStat.getCreated(),ipvStat.getType(),ipvStat.getClientType()
            };
            updateParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,updateSQL, updateParamsList);
    }
}
