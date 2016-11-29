package com.yonyou.jdbc;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * Created by ChenXiaoLei on 2016/11/8.
 */
public class JDBCHelper {
    public static int executeUpdate(Connection conn,String sql, Object[] params) {
        int rtn = 0;
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
            if(params != null && params.length > 0) {
                for(int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
//          System.out.println(pstmt.toString());
            rtn = pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rtn;
    }
    /**
     * 执行查询SQL语句
     * @param sql
     * @param params
     * @param callback
     */
    public static void executeQuery(Connection conn,String sql, Object[] params,
                             QueryCallback callback) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = conn.prepareStatement(sql);
            if(params != null && params.length > 0) {
                for(int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
//             System.out.println(pstmt.toString());
            rs = pstmt.executeQuery();
            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 批量执行SQL语句
     * @param sql
     * @param paramsList
     * @return 每条SQL语句影响的行数
     */
    public static int[] executeBatch(Connection conn,String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        PreparedStatement pstmt = null;
            try {
//            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);
            if(paramsList != null && paramsList.size() > 0) {
                for(Object[] params : paramsList) {
                    for(int i = 0; i < params.length; i++) {
                        pstmt.setObject(i + 1, params[i]);
                    }
                    pstmt.addBatch();
                }
            }
            rtn = pstmt.executeBatch();
//            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rtn;
    }
    /**
     * 静态内部类：查询回调接口
     * @author Administrator
     *
     */
    public static interface QueryCallback {
        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }
}
