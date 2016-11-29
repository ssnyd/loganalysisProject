package com.yonyou.dao.impl;

import com.yonyou.dao.ILogStatDAO;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.model.PVStatQueryResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ChenXiaoLei on 2016/11/9.
 */
public class LogStatDAOImpl implements ILogStatDAO{
    public void updataBatch(List<Map<String, String>> pvStats, Connection conn) {

        List<Map<String, String>> insertPVStats = new ArrayList<Map<String, String>>();
        List<Map<String, String>> updatePVStats = new ArrayList<Map<String, String>>();
        String selectSQL = "SELECT count(*) "
                            + "FROM rp_pv_count "
                            + "WHERE created=? "
                            + "AND clientType=? ";
        for (Map<String, String> pvStat : pvStats) {
            final PVStatQueryResult queryResult = new PVStatQueryResult();
                Object[] params = new Object[]{
                        pvStat.get("timestamp")==null?"0":pvStat.get("timestamp"),
                        "all"
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
                    updatePVStats.add(pvStat);
                } else {
                    insertPVStats.add(pvStat);
                }
        }
            // 执行插入操作 38个参数
            String insertSQL = "INSERT INTO rp_pv_count(clientType,created,haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            List<Object[]> insertParamsList = new ArrayList<Object[]>();
            for (Map<String, String> map :insertPVStats){
                Object[] params = getParam(map);
                insertParamsList.add(params);
            }
            JDBCHelper.executeBatch(conn,insertSQL, insertParamsList);
            // 对于需要更新的数据，执行更新操作
        final int[] num = new int[36];
        StringBuffer updateSQL = new StringBuffer();
        updateSQL.append("UPDATE rp_pv_count set ");
        String selectsql = "select haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local from rp_pv_count where created = ";
        for (Map<String, String> map :updatePVStats){
            selectsql = selectsql+map.get("timestamp");
            JDBCHelper.executeQuery(conn,selectsql,new Object[]{},new JDBCHelper.QueryCallback(){
                public void process(ResultSet rs) throws Exception {
                    while (rs.next()){
                        for (int i =1 ;i<37;i++){
                            rs.getInt(i);
                            num[i-1] = rs.getInt(i);
                        }
                    }
                }
            });
            for ( Map.Entry entry : map.entrySet()){
                String city = entry.getKey().toString();
                if (!"timestamp".equals(city)){
                    String count = entry.getValue().toString();
                    updateSQL.append(city).append("=").append(getCount(city,count,num)).append(",");
                }
            }
            String updatesql = updateSQL.substring(0, updateSQL.length() - 1) +" WHERE created =" + map.get("timestamp");
            JDBCHelper.executeUpdate(conn,updatesql, new Object[]{});
        }
    }

    private int getCount(String city, String count, int[] num) {
        int c = 0;
        String citys = "haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local";
        String[] result = citys.split(",");
        for (int i = 0 ;i<36;i++) {
            if (result[i].equals(city)){
                c = Integer.parseInt(count) +num[i];
                break;
            }
        }
        return c ;
    }

    /**
     * 获得插入的参数
     * @param map
     * @return
     */
    private Object[] getParam(Map<String, String> map) {
        Object[] params = new Object[38];
        params[0] = "all";
        params[1] = map.get("timestamp");
        String citys = "haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local";
        String[] city = citys.split(",");
        for (int i =2 ;i < 38;i++) {
            String region = city[i - 2];
            params[i] = map.get(region);
            if (params[i] == null || "".equals(params[i])){
                params[i] = "0";
            }
        }
        return params;
    }
}


























//package com.yonyou.dao.impl;
//
//import com.yonyou.dao.ILogStatDAO;
//import com.yonyou.jdbc.dbutils.JdbcHelper;
//
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by ChenXiaoLei on 2016/11/9.
// */
//public class LogStatDAOImpl implements ILogStatDAO{
//    public static Object object = new Object();
//    public void updataBatch(List<Map<String, String>> pvStats) {
//        List<Map<String, String>> insertPVStats = new ArrayList<Map<String, String>>();
//        List<Map<String, String>> updatePVStats = new ArrayList<Map<String, String>>();
//        for (Map<String, String> pvStat : pvStats) {
//            try {
//                if(!("".equals(pvStat.get("timestamp"))|| pvStat.get("timestamp")==null)){
//                    String selectSQL = "SELECT count(*) "
//                            + "FROM rp_pv_count "
//                            + "WHERE created=? "
//                            + "AND clientType=? ";
//                    Object[] params = new Object[]{
//                            pvStat.get("timestamp"),
//                            "all"
//                    };
//                    Object single = JdbcHelper.getSingle(selectSQL, params);
//                    int count = Integer.parseInt(String.valueOf(single));
//                    if(count > 0) {
//                        updatePVStats.add(pvStat);
//                    } else {
//                        insertPVStats.add(pvStat);
//                    }
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//        if (insertPVStats.size()>0){
//            try {
//                // 执行插入操作 38个参数
//                String insertSQL = "INSERT INTO rp_pv_count(clientType,created,haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
//                List<Object[]> insertParamsList = new ArrayList<Object[]>();
//                for (Map<String, String> map :insertPVStats){
//                    Object[] params = getParam(map);
//                    insertParamsList.add(params);
//            }
//                JdbcHelper.insertWithBatch(insertSQL, insertParamsList);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//        if (updatePVStats.size() > 0) {
//            // 对于需要更新的数据，执行更新操作
//            final int[] num = new int[36];
//            try {
//                for (Map<String, String> map :updatePVStats){
//                    StringBuffer updateSQL = new StringBuffer();
//                    updateSQL.append("UPDATE rp_pv_count set ");
//                    String selectsql = "select haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local from rp_pv_count where created = "+map.get("timestamp");
//                    JdbcHelper.queryResult(selectsql,new Object[0],new JdbcHelper.QueryCallback(){
//                        public void process(ResultSet rs) throws Exception {
//                            while (rs.next()){
//                                for (int i =1 ;i<37;i++){
//                                    rs.getInt(i);
//                                    num[i-1] = rs.getInt(i);
//                                }
//                            }
//                        }
//                    });
//                    for ( Map.Entry entry : map.entrySet()){
//                        String city = entry.getKey().toString();
//                        if (!"timestamp".equals(city)){
//                            String count = entry.getValue().toString();
//                            updateSQL.append(city).append("=").append(getCount(city,count,num)).append(",");
//                        }
//                    }
//                    String updatesql = updateSQL.substring(0, updateSQL.length() - 1) +" WHERE created =" + map.get("timestamp");
//                    JdbcHelper.insertWithReturnPrimeKey(updatesql, new Object[]{});
//                }
//            }catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    private int getCount(String city, String count, int[] num) {
//        int c = 0;
//        String citys = "haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local";
//        String[] result = citys.split(",");
//        for (int i = 0 ;i<36;i++) {
//            if (result[i].equals(city)){
//                c = Integer.parseInt(count) +num[i];
//                break;
//            }
//        }
//        return c ;
//    }
//
//    /**
//     * 获得插入的参数
//     * @param map
//     * @return
//     */
//    private Object[] getParam(Map<String, String> map) {
//        Object[] params = new Object[38];
//        params[0] = "all";
//        params[1] = map.get("timestamp");
//        String citys = "haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local";
//        String[] city = citys.split(",");
//        for (int i =2 ;i < 38;i++) {
//            String region = city[i - 2];
//            params[i] = map.get(region);
//            if (params[i] == null || "".equals(params[i])){
//                params[i] = "0";
//            }
//        }
//        return params;
//    }
//}


//package com.yonyou.dao.impl;
//
//import com.yonyou.dao.ILogStatDAO;
//import com.yonyou.entity.PVStat;
//import com.yonyou.jdbc.JDBCHelper;
//import com.yonyou.jdbc.model.PVStatQueryResult;
//
//import java.sql.ResultSet;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by ChenXiaoLei on 2016/11/9.
// */
//public class LogStatDAOImpl implements ILogStatDAO{
//    public static Object object = new Object();
//    public void updataBatch(List<PVStat> pvStats) {
//        synchronized (object){
//            JDBCHelper jdbcHelper = JDBCHelper.getInstance();
//            List<PVStat> insertPVStats = new ArrayList<PVStat>();
//            List<PVStat> updatePVStats = new ArrayList<PVStat>();
//            for (PVStat pvStat : pvStats) {
//                if(!"".equals(pvStat.getRegion())){
//                    String selectSQL = "SELECT count(*) "
//                            + "FROM rp_pv_count "
//                            + "WHERE created=? "
//                            + "AND clientType=? ";
//                    final PVStatQueryResult queryResult = new PVStatQueryResult();
//                    Object[] params = new Object[]{
//                            pvStat.getTimestamp(),
//                            "all"
//                    };
//                    jdbcHelper.executeQuery(selectSQL,params,new JDBCHelper.QueryCallback(){
//                        public void process(ResultSet rs) throws Exception {
//                            if (rs.next()){
//                                int count = rs.getInt(1);
//                                queryResult.setCount(count);
//                            }
//                        }
//                    }
//                    );
//                    int count = queryResult.getCount();
//                    System.out.println(count+"BBBBBBBBBBBBBBBBBBBBB");
//                    if(count > 0) {
//
//                        updatePVStats.add(pvStat);
//                    } else {
//                        insertPVStats.add(pvStat);
//                    }
//                }
//                // 执行插入操作
//                for (PVStat map :insertPVStats){
//                    String insertSQL = "INSERT INTO rp_pv_count(clientType,created,"+map.getRegion()+") VALUES(?,?,?)";
//                    Object[] params = new Object[]{
//                            "all",map.getTimestamp(),map.getClickCount()
//                    };
//                    jdbcHelper.executeUpdate(insertSQL, params);
//
//                }
//                insertPVStats.clear();
//                // 对于需要更新的数据，执行更新操作
//                for (PVStat map :updatePVStats){
//                    String updateSQL = "UPDATE rp_pv_count set "+map.getRegion()+"=? "
//                            + "WHERE created=? ";
//                    Object[] params = new Object[]{
//                            map.getClickCount(),map.getTimestamp()
//                    };
//                    jdbcHelper.executeUpdate(updateSQL, params);
//                }
//                updatePVStats.clear();
//
//            }
//        }
//    }
//    /**
//     * 获取params 数据库用
//     * @param map
//     * @return
//     */
//    private Object[] getParams(PVStat map , int num){
//        if (num == 0){
//            Object[] params = new Object[38];
//            params[0] = "all";
//            params[1] = map.getTimestamp();
//            String citys = "haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local";
//            String[] city = citys.split(",");
//            for (int i =2 ;i < 38;i++){
//                String region = city[i - 2];
//                if (region.equals(map.getRegion())){
//                    params[i]=map.getClickCount();
//                }
//                if (params[i] == null || "".equals(params[i])){
//                    params[i] = "0";
//                }
//            }
//            return params;
//        }else {
//            Object[] params = new Object[36];
//            String citys = "haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local";
//            String[] city = citys.split(",");
//            for (int i =0 ;i < 36;i++){
//                String region = city[i];
//                if (region.equals(map.getRegion())){
//                    params[i]=map.getClickCount();
//                }
//                if (params[i] == null || "".equals(params[i])){
//                    params[i] = "0";
//                }
//            }
//            params[35] = map.getTimestamp();
//            return params;
//        }
//    }
//}




//package com.yonyou.dao.impl;
//
//import com.yonyou.dao.ILogStatDAO;
//import com.yonyou.entity.PVStat;
//import com.yonyou.jdbc.JDBCHelper;
//import com.yonyou.jdbc.model.PVStatQueryResult;
//
//import java.sql.ResultSet;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by ChenXiaoLei on 2016/11/9.
// */
//public class LogStatDAOImpl implements ILogStatDAO{
//    public static Object object = new Object();
//    public void updataBatch(List<PVStat> pvStats) {
//        synchronized (object){
//            JDBCHelper jdbcHelper = JDBCHelper.getInstance();
//            List<PVStat> insertPVStats = new ArrayList<PVStat>();
//            List<PVStat> updatePVStats = new ArrayList<PVStat>();
//            for (PVStat pvStat : pvStats) {
//                if(!"".equals(pvStat.getRegion())){
//                    String selectSQL = "SELECT count(*) "
//                            + "FROM rp_pv_count "
//                            + "WHERE created=? "
//                            + "AND clientType=? ";
//                    final PVStatQueryResult queryResult = new PVStatQueryResult();
//                    Object[] params = new Object[]{
//                            pvStat.getTimestamp(),
//                            "all"
//                    };
//                    jdbcHelper.executeQuery(selectSQL,params,new JDBCHelper.QueryCallback(){
//                        public void process(ResultSet rs) throws Exception {
//                            if (rs.next()){
//                                int count = rs.getInt(1);
//                                queryResult.setCount(count);
//                            }
//                        }
//                    }
//                    );
//                    int count = queryResult.getCount();
//                    System.out.println(count+"BBBBBBBBBBBBBBBBBBBBB");
//                    if(count > 0) {
//
//                        updatePVStats.add(pvStat);
//                    } else {
//                        insertPVStats.add(pvStat);
//                    }
//                }
//                // 执行插入操作
//                for (PVStat map :insertPVStats){
//                    String insertSQL = "INSERT INTO rp_pv_count(clientType,created,"+map.getRegion()+") VALUES(?,?,?)";
//                    Object[] params = new Object[]{
//                            "all",map.getTimestamp(),map.getClickCount()
//                    };
//                    jdbcHelper.executeUpdate(insertSQL, params);
//
//                }
//                insertPVStats.clear();
//                // 对于需要更新的数据，执行更新操作
//                for (PVStat map :updatePVStats){
//                    String updateSQL = "UPDATE rp_pv_count set "+map.getRegion()+"=? "
//                            + "WHERE created=? ";
//                    Object[] params = new Object[]{
//                            map.getClickCount(),map.getTimestamp()
//                    };
//                    jdbcHelper.executeUpdate(updateSQL, params);
//                }
//                updatePVStats.clear();
//
//            }
//        }
//    }
//    /**
//     * 获取params 数据库用
//     * @param map
//     * @return
//     */
//    private Object[] getParams(PVStat map , int num){
//        if (num == 0){
//            Object[] params = new Object[38];
//            params[0] = "all";
//            params[1] = map.getTimestamp();
//            String citys = "haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local";
//            String[] city = citys.split(",");
//            for (int i =2 ;i < 38;i++){
//                String region = city[i - 2];
//                if (region.equals(map.getRegion())){
//                    params[i]=map.getClickCount();
//                }
//                if (params[i] == null || "".equals(params[i])){
//                    params[i] = "0";
//                }
//            }
//            return params;
//        }else {
//            Object[] params = new Object[36];
//            String citys = "haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local";
//            String[] city = citys.split(",");
//            for (int i =0 ;i < 36;i++){
//                String region = city[i];
//                if (region.equals(map.getRegion())){
//                    params[i]=map.getClickCount();
//                }
//                if (params[i] == null || "".equals(params[i])){
//                    params[i] = "0";
//                }
//            }
//            params[35] = map.getTimestamp();
//            return params;
//        }
//    }
//}
//
//
////package com.yonyou.dao.impl;
////
////import com.yonyou.dao.ILogStatDAO;
////import com.yonyou.entity.PVStat;
////import com.yonyou.jdbc.JDBCHelper;
////import com.yonyou.jdbc.model.PVStatQueryResult;
////
////import java.sql.ResultSet;
////import java.util.ArrayList;
////import java.util.List;
////import java.util.Map;
////
/////**
//// * Created by ChenXiaoLei on 2016/11/9.
//// */
////public class LogStatDAOImpl implements ILogStatDAO{
////    public static Object object = new Object();
////    public void updataBatch(List<PVStat> pvStats) {
////        synchronized (object){
////            JDBCHelper jdbcHelper = JDBCHelper.getInstance();
////            List<PVStat> insertPVStats = new ArrayList<PVStat>();
////            List<PVStat> updatePVStats = new ArrayList<PVStat>();
////            for (PVStat pvStat : pvStats) {
////                if(!"".equals(pvStat.getRegion())){
////                    String selectSQL = "SELECT count(*) "
////                            + "FROM rp_pv_count "
////                            + "WHERE created=? "
////                            + "AND clientType=? ";
////                    final PVStatQueryResult queryResult = new PVStatQueryResult();
////                    Object[] params = new Object[]{
////                            pvStat.getTimestamp(),
////                            "all"
////                    };
////                    jdbcHelper.executeQuery(selectSQL,params,new JDBCHelper.QueryCallback(){
////                        public void process(ResultSet rs) throws Exception {
////                            if (rs.next()){
////                                int count = rs.getInt(1);
////                                queryResult.setCount(count);
////                            }
////                        }
////                    }
////                    );
////                    int count = queryResult.getCount();
////                    System.out.println(count+"BBBBBBBBBBBBBBBBBBBBB");
////                    if(count > 0) {
////
////                        updatePVStats.add(pvStat);
////                    } else {
////                        insertPVStats.add(pvStat);
////                    }
////                }
////                // 执行插入操作
////                for (PVStat map :insertPVStats){
////                    String insertSQL = "INSERT INTO rp_pv_count(clientType,created,"+map.getRegion()+") VALUES(?,?,?)";
////                    Object[] params = new Object[]{
////                            "all",map.getTimestamp(),map.getClickCount()
////                    };
////                    jdbcHelper.executeUpdate(insertSQL, params);
////
////                }
////                insertPVStats.clear();
////                // 对于需要更新的数据，执行更新操作
////                for (PVStat map :updatePVStats){
////                    String updateSQL = "UPDATE rp_pv_count set "+map.getRegion()+"=? "
////                            + "WHERE created=? ";
////                    Object[] params = new Object[]{
////                            map.getClickCount(),map.getTimestamp()
////                    };
////                    jdbcHelper.executeUpdate(updateSQL, params);
////                }
////                updatePVStats.clear();
////
////            }
////        }
////    }
////    /**
////     * 获取params 数据库用
////     * @param map
////     * @return
////     */
////    private Object[] getParams(PVStat map , int num){
////        if (num == 0){
////            Object[] params = new Object[38];
////            params[0] = "all";
////            params[1] = map.getTimestamp();
////            String citys = "haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local";
////            String[] city = citys.split(",");
////            for (int i =2 ;i < 38;i++){
////                String region = city[i - 2];
////                if (region.equals(map.getRegion())){
////                    params[i]=map.getClickCount();
////                }
////                if (params[i] == null || "".equals(params[i])){
////                    params[i] = "0";
////                }
////            }
////            return params;
////        }else {
////            Object[] params = new Object[36];
////            String citys = "haiwai,aomen,xianggang,taiwan,guangdong,guangxi,hainan,yunnan,fujian,jiangxi,hunan,guizhou,zhejiang,anhui,shanghai,jiangsu,hubei,xizang,qinghai,gansu,xinjiang,shanxi_shan,henan,shanxi_jin,shandong,hebei,tianjin,beijing,ningxia,neimeng,liaoning,jilin,heilongjiang,chongqing,sichuan,local";
////            String[] city = citys.split(",");
////            for (int i =0 ;i < 36;i++){
////                String region = city[i];
////                if (region.equals(map.getRegion())){
////                    params[i]=map.getClickCount();
////                }
////                if (params[i] == null || "".equals(params[i])){
////                    params[i] = "0";
////                }
////            }
////            params[35] = map.getTimestamp();
////            return params;
////        }
////    }
////}
