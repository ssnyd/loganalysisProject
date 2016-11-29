package com.yonyou.dao.impl;

import com.yonyou.dao.IUVStatDAO;
import com.yonyou.entity.UVStat;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.model.PVStatQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/16.
 */
public class UVStatDAOImpl implements IUVStatDAO {
    @Override
    public void updataBatch(List<UVStat> uvStats, Connection conn) {
        List<UVStat> insertUVStats = new ArrayList<UVStat>();
        List<UVStat> updateUVStats = new ArrayList<UVStat>();
        String selectSQL = "SELECT count(*) "
                + "FROM rp_uv_count "
                + "WHERE created=? "
                + "AND clientType=? "
                + "AND type=? ";
        for (UVStat uvStat : uvStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
            Object[] params = new Object[]{
                    uvStat.getCreated(),uvStat.getClientType(),uvStat.getType()
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
                updateUVStats.add(uvStat);
            } else {
                insertUVStats.add(uvStat);
            }
        }
        //666666666666666666666666666666666666666666666666666
        // 执行插入操作 4个参数
            String insertSQL = "INSERT INTO rp_uv_count(type,clientType,created,num) VALUES(?,?,?,?)";
            List<Object[]> insertParamsList = new ArrayList<Object[]>();
            for (UVStat uvStat :insertUVStats){
               Object[] param =  new Object[]{
                    uvStat.getType(),uvStat.getClientType(),uvStat.getCreated(),uvStat.getNum()
                };
                insertParamsList.add(param);
            }
            JDBCHelper.executeBatch(conn,insertSQL, insertParamsList);
        //执行更新操作 需要更新数据 做查询
        String updateSQL = "UPDATE rp_uv_count set "
                + "num = ? "
                 + "WHERE created=? "
                 + "AND clientType=? "
                 + "AND type=? ";
        List<Object[]> updateParamsList = new ArrayList<Object[]>();
        for (UVStat uvStat:updateUVStats){
            Object[] param =  new Object[]{
                    uvStat.getNum(),uvStat.getCreated(),uvStat.getClientType(),uvStat.getType()
                };
                updateParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,updateSQL, updateParamsList);
    }
}
