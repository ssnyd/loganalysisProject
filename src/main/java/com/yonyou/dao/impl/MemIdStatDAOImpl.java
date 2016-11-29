package com.yonyou.dao.impl;

import com.yonyou.dao.IMemIdStatDAO;
import com.yonyou.entity.MemIDStat;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.model.PVStatQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/16.
 */
public class MemIdStatDAOImpl implements IMemIdStatDAO {
    @Override
    public void updataBatch(List<MemIDStat> memIDStats, Connection conn) {
        List<MemIDStat> insertMemIDStats = new ArrayList<MemIDStat>();
        List<MemIDStat> updateMemIDStats = new ArrayList<MemIDStat>();
        String selectSQL = "SELECT count(*) "
                + "FROM rp_uv_member "
                + "WHERE created=? "
                + "AND qzId=? "
                + "AND memberId=? ";
        for (MemIDStat memIDStat : memIDStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
            Object[] params = new Object[]{
                    memIDStat.getCreated(),memIDStat.getQzId(), memIDStat.getMemberId()
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
                updateMemIDStats.add(memIDStat);
            } else {
                insertMemIDStats.add(memIDStat);
            }
        }
        //6666666666666666666666666666666666666666666666666666666666666666666
        // 执行插入操作 3个参数
            String insertSQL = "INSERT INTO rp_uv_member(created,qzId,memberId,times) VALUES(?,?,?,?)";
            List<Object[]> insertParamsList = new ArrayList<Object[]>();
            for (MemIDStat memIDStat : insertMemIDStats){
               Object[] param =  new Object[]{
                    memIDStat.getCreated(),memIDStat.getQzId(), memIDStat.getMemberId(), memIDStat.getTimes()
                };
                insertParamsList.add(param);
            }
            JDBCHelper.executeBatch(conn,insertSQL, insertParamsList);
        //执行更新操作 需要更新数据 做查询
        String updateSQL = "UPDATE rp_uv_member set "
                + "times = ? "
                 + "WHERE created=? "
                 + "AND qzId=? "
                + "AND memberId=? ";
        List<Object[]> updateParamsList = new ArrayList<Object[]>();
        for (MemIDStat memIDStat:updateMemIDStats){
            Object[] param =  new Object[]{
                    memIDStat.getTimes(),memIDStat.getCreated(),memIDStat.getQzId(),memIDStat.getMemberId()
            };
            updateParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn,updateSQL, updateParamsList);
    }
}
