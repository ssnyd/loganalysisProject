package com.yonyou.dao.impl;

import com.yonyou.dao.IPayEUUVDAO;
import com.yonyou.entity.enterprise.EVStat;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.model.PVStatQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenxiaolei on 17/1/23.
 */
public class PayEUUVDAOImpl implements IPayEUUVDAO {
    @Override
    public List<String> findAll(Connection conn, String start, String end) {
        String sql = "select * from rp_enterprise_payment where start <= ? and end >= ? and totalFee > 0";

        final List<String> lists = new ArrayList<String>();
        Object[] params = new Object[]{
                start, end
        };
        JDBCHelper.executeQuery(conn, sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {
                    String ins = String.valueOf(rs.getInt(2));
                    lists.add(ins);
                }
            }
        });
        return lists;
    }

    //存uv ev
    @Override
    public void updataBatch(List<EVStat> evStats, Connection conn, int flag) {
        String selectSQL = null;
        String insertSQL = null;
        String updateSQL = null;
        if (flag == 0) {
            selectSQL = "SELECT count(*) "
                    + "FROM rp_pay_ev_num "
                    + "WHERE created=? "
                    + "AND type=? ";
            insertSQL = "INSERT INTO rp_pay_ev_num(type,created,num) VALUES(?,?,?)";
            updateSQL = "UPDATE rp_pay_ev_num set "
                    + "num = ? "
                    + "WHERE created=? "
                    + "AND type=? ";
        } else if (flag == 1) {
            selectSQL = "SELECT count(*) "
                    + "FROM rp_pay_uv_count "
                    + "WHERE created=? "
                    + "AND type=? ";
            insertSQL = "INSERT INTO rp_pay_uv_count(type,created,num) VALUES(?,?,?)";
            updateSQL = "UPDATE rp_pay_uv_count set "
                    + "num = ? "
                    + "WHERE created=? "
                    + "AND type=? ";
        }
        List<EVStat> insertEVStats = new ArrayList<EVStat>();
        List<EVStat> updateEVStats = new ArrayList<EVStat>();
        for (EVStat evStat : evStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
            Object[] params = new Object[]{
                    evStat.getCreated(), evStat.getType()
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
                updateEVStats.add(evStat);
            } else {
                insertEVStats.add(evStat);
            }
        }
        //666666666666666666666666666666666666666666666666666
        // 执行插入操作 3个参数

        List<Object[]> insertParamsList = new ArrayList<Object[]>();
        for (EVStat evStat : insertEVStats) {
            Object[] param = new Object[]{
                    evStat.getType(), evStat.getCreated(), evStat.getNum()
            };
            insertParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn, insertSQL, insertParamsList);
        //执行更新操作 需要更新数据 做查询

        List<Object[]> updateParamsList = new ArrayList<Object[]>();
        for (EVStat evStat : updateEVStats) {
            Object[] param = new Object[]{
                    evStat.getNum(), evStat.getCreated(), evStat.getType()
            };
            updateParamsList.add(param);
        }
        JDBCHelper.executeBatch(conn, updateSQL, updateParamsList);
    }
}
