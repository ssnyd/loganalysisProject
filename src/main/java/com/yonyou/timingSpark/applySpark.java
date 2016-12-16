package com.yonyou.timingSpark;

import com.yonyou.dao.IApplyStatDAO;
import com.yonyou.dao.factory.DAOFactory;
import com.yonyou.entity.applyStat;
import com.yonyou.jdbc.JDBCHelper;
import com.yonyou.jdbc.JDBCUtils;
import com.yonyou.jdbc.model.PVStatQueryResult;
import com.yonyou.utils.DateUtils;
import com.yonyou.utils.HttpReqUtil;
import com.yonyou.utils.JSONUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ChenXiaoLei on 2016/11/25.
 */
public class applySpark {
    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("applySpark")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//      sconf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("app_case"));
        scan.addColumn(Bytes.toBytes("app_case"), Bytes.toBytes("log"));
//      scan.setStartRow(Bytes.toBytes(getTimes("2016:11:28")+":#"));
//      scan.setStopRow(Bytes.toBytes(getTimes("2016:11:28")+"::"));
        if (args.length==2){
            scan.setStartRow(Bytes.toBytes(getTimes(args[0]) + ":#"));
            scan.setStopRow(Bytes.toBytes(getTimes(args[1]) + "::"));
        }else {
            scan.setStartRow(Bytes.toBytes(getTimes(DateUtils.getYesterdayDate()) + ":#"));
            scan.setStopRow(Bytes.toBytes(getTimes(DateUtils.getYesterdayDate()) + "::"));
        }

        try {
            final String tableName = "esn_datacollection";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                    sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class).repartition(200);
            //读取的每一行数据
            JavaRDD<String> filterRDD = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                @Override
                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {

                    byte[] value = v1._2.getValue(Bytes.toBytes("app_case"), Bytes.toBytes("log"));
                    if (value != null) {
                        return Bytes.toString(value);
                    }
                    return null;
                }
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String v1) throws Exception {
                    return v1 != null;
                }
            }).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
                @Override
                public Iterable<String> call(Iterator<String> iterator) throws Exception {
                    List<String> list = new ArrayList<>();
                    String line = "";
                    while (iterator.hasNext()) {
                        //action:view&app_id:22239&instance_id:3219&qz_id:3968&member_id:3469&mtime:1480044831884
                        line = JSONUtil.getappId(iterator.next());
                        list.add(line);
                    }
                    return list;
                }
            });

            //获得openid
            JavaRDD<String> openIdRDD = filterRDD.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String line) throws Exception {
                    return line.split("&").length > 2 && line.split("&")[1].split(":").length == 2;
                }
            }).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
                @Override
                public Iterable<String> call(Iterator<String> iterator) throws Exception {
                    List<String> list = new ArrayList<>();
                    String app_id = "";
                    String line = "";
                    while (iterator.hasNext()) {
                        line = iterator.next();
                        app_id = line.split("&")[1].split(":")[1];
                        if (app_id.contains("-")) {
                            String[] str = line.split("&");
                            line = "open_appid:"+app_id + "&" + "name:empty" + "&" + str[0] + "&" + "app_id:0" + "&" + str[2] + "&" + str[3] + "&" + str[4] + "&" + str[5];
                        } else {
                            String opid = JSONUtil.getopenId(HttpReqUtil.getResult("app/info/" + app_id, ""));
                            line = opid + "&" + line;
                        }
                        //open_appid:110&name:协同日程新&action:view&app_id:22239&instance_id:3219&qz_id:3968&member_id:3469&mtime:1480044831884
                        list.add(line);
                    }
                    return list;
                }
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {

                    return s.split("&")[1].split(":").length == 2 && s.split("&")[0].split(":").length == 2;
                }
            }).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
                @Override
                public Iterable<String> call(Iterator<String> iterator) throws Exception {
                    List list = new ArrayList();
                    String app_id = null;
                    String line = null;
                    while (iterator.hasNext()) {
                        line = iterator.next();
                        String[] str = line.split("&");
                        if (!"0".equals(str[0].split(":")[1])) {
                            app_id = "app_id:0";
                            line = str[0] + "&" + str[1] + "&" + str[2] + "&" + app_id + "&" + str[4] + "&" + str[5] + "&" + str[6] + "&" + str[7];
                        }
                        list.add(line);
                    }
                    return list;
                }
            });

            //获得rpid 通过mysql获得
            JavaRDD<String> reipRDD = openIdRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
                @Override
                public Iterable<String> call(Iterator<String> iterator) throws Exception {
                    String selectSQL = "SELECT id "
                            + "FROM rp_app_general "
                            + "WHERE appId=? "
                            + "AND openAppId=? ";
                    String insertSQL = "INSERT INTO rp_app_general(appId,openAppId,name,created) "
                            + "VALUES(?,?,?,?)";
                    List<String> list = new ArrayList<>();
                    JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                    Connection conn = jdbcUtils.getConnection();
                    String line = "";
                    while (iterator.hasNext()) {
                        final PVStatQueryResult queryResult = new PVStatQueryResult();
                        line = iterator.next();
                        String[] lines = line.split("&");
                        String app_id = lines[3].split(":")[1];
                        String open_id = lines[0].split(":")[1];
                        String name = lines[1].split(":")[1];
                        JDBCHelper.executeQuery(conn, selectSQL, new Object[]{
                                app_id, open_id
                        }, new JDBCHelper.QueryCallback() {
                            @Override
                            public void process(ResultSet rs) throws Exception {
                                if (rs.next()) {
                                    int count = rs.getInt(1);
                                    queryResult.setCount(count);
                                }
                            }
                        });
                        int rpid = queryResult.getCount();
                        if (rpid == 0) {
                            JDBCHelper.executeUpdate(conn, insertSQL, new Object[]{
                                    app_id, open_id, name, new Date().getTime() / 1000
                            });
                            JDBCHelper.executeQuery(conn, selectSQL, new Object[]{
                                    app_id, open_id
                            }, new JDBCHelper.QueryCallback() {
                                @Override
                                public void process(ResultSet rs) throws Exception {
                                    if (rs.next()) {
                                        int count = rs.getInt(1);
                                        queryResult.setCount(count);
                                    }
                                }
                            });
                            rpid = queryResult.getCount();
                        }
                        line = "reid:" + rpid + "&" + line;
                        list.add(line);
                    }
                    if (conn != null) {
                        jdbcUtils.closeConnection(conn);
                    }
                    //reid:2 0 &open_appid:110 1 &name:协同日程新 2 &action:view 3 &app_id:22239 4 &instance_id:3219 5 &qz_id:3968 6 &member_id:3469 7 &mtime:1480044831884 8
                    return list;
                }
            });
            JavaPairRDD<String, String> finshRDD = reipRDD.mapToPair(new PairFunction<String, String, String>() {
                @Override
                public Tuple2<String, String> call(String s) throws Exception {
                    //时间+rp+action s
                    String[] str = s.split("&");
                    long time = getDays(str[8].split(":")[1]);
                    String rp = str[0].split(":")[1];
                    String action = str[3].split(":")[1];
                    return new Tuple2<String, String>(time + "&" + rp + "&" + action, s);
                }
            });
            finshRDD = finshRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
            //num
            JavaPairRDD<String, Integer> numrdd = finshRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                    return new Tuple2<String, Integer>(tuple2._1, 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });

            //666666666666666
            //Map<String, Object> numMap = finshRDD.countByKey();
            //if (!numMap.isEmpty()){
            //    List<applyStat> applyStats = new ArrayList<applyStat>();
            //    for (Map.Entry<String, Object> map:numMap.entrySet()){
            //        applyStat applyStat = new applyStat();
            //        Object value = map.getValue();
            //        Integer category = 0;
            //        if (value instanceof Integer){
            //            category = (Integer)value;
            //        }
            //        String key = map.getKey();
            //        String[] str = key.split("&");
            //        String time = str[0];
            //        String rpid = str[1];
            //        String action = str[2];
            //        applyStat.setAction(action);
            //        applyStat.setCategory(category);
            //        applyStat.setCreated(time);
            //        applyStat.setRpid(rpid);
            //        applyStats.add(applyStat);
            //    }
            //    if (applyStats.size()>0){
            //        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
            //        Connection conn = jdbcUtils.getConnection();
            //        IApplyStatDAO applyStatDAO = DAOFactory.getApplyStatDAO();
            //        applyStatDAO.updataBatch(applyStats,conn,3);
            //        System.out.println("mysql 3 applystat num==> "+applyStats.size());
            //        applyStats.clear();
            //        if (conn != null) {
            //            jdbcUtils.closeConnection(conn);
            //        }
            //    }
            //
            //}


            //instanceRDD
            final JavaPairRDD<String, Integer> instanceRDD = finshRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                    return new Tuple2<String, String>(tuple2._1, tuple2._2.split("&")[5].split(":")[1]);
                }
            }).distinct().mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                    return new Tuple2<String, Integer>(tuple2._1, 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });
            //qzRDD
            JavaPairRDD<String, Integer> qzRDD = finshRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                    return new Tuple2<String, String>(tuple2._1, tuple2._2.split("&")[6].split(":")[1]);
                }
            }).distinct().mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                    return new Tuple2<String, Integer>(tuple2._1, 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });
            //memRDD
            JavaPairRDD<String, Integer> memRDD = finshRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                    return new Tuple2<String, String>(tuple2._1, tuple2._2.split("&")[7].split(":")[1]);
                }
            }).distinct().mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                    return new Tuple2<String, Integer>(tuple2._1, 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });

            memRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                @Override
                public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                    List<applyStat> applyStats = new ArrayList<applyStat>();
                    Tuple2<String, Integer> tuple = null;
                    while (iterator.hasNext()) {
                        tuple = iterator.next();
                        String[] str = tuple._1.split("&");
                        String time = str[0];
                        String rpid = str[1];
                        String action = str[2];
                        Integer category = tuple._2;
                        applyStat applyStat = new applyStat();
                        System.out.print("");
                        applyStat.setAction(action);
                        applyStat.setCategory(category);
                        applyStat.setCreated(time);
                        applyStat.setRpid(rpid);
                        applyStats.add(applyStat);
                    }
                    if (applyStats.size() > 0) {
                        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                        Connection conn = jdbcUtils.getConnection();
                        IApplyStatDAO applyStatDAO = DAOFactory.getApplyStatDAO();
                        applyStatDAO.updataBatch(applyStats, conn, 2);
                        System.out.println("mysql 2 applystat mem==> " + applyStats.size());
                        applyStats.clear();
                        if (conn != null) {
                            jdbcUtils.closeConnection(conn);
                        }
                    }
                }
            });
            qzRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                @Override
                public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                    List<applyStat> applyStats = new ArrayList<applyStat>();
                    Tuple2<String, Integer> tuple = null;
                    while (iterator.hasNext()) {
                        tuple = iterator.next();
                        String[] str = tuple._1.split("&");
                        String time = str[0];
                        String rpid = str[1];
                        String action = str[2];
                        Integer category = tuple._2;
                        System.out.print("");
                        applyStat applyStat = new applyStat();
                        applyStat.setAction(action);
                        applyStat.setCategory(category);
                        applyStat.setCreated(time);
                        applyStat.setRpid(rpid);
                        applyStats.add(applyStat);
                    }
                    if (applyStats.size() > 0) {
                        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                        Connection conn = jdbcUtils.getConnection();
                        IApplyStatDAO applyStatDAO = DAOFactory.getApplyStatDAO();
                        applyStatDAO.updataBatch(applyStats, conn, 1);
                        System.out.println("mysql 2 applystat qz==> " + applyStats.size());
                        applyStats.clear();
                        if (conn != null) {
                            jdbcUtils.closeConnection(conn);
                        }
                    }
                }
            });
            instanceRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                @Override
                public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                    List<applyStat> applyStats = new ArrayList<applyStat>();
                    Tuple2<String, Integer> tuple = null;
                    while (iterator.hasNext()) {
                        tuple = iterator.next();
                        String[] str = tuple._1.split("&");
                        String time = str[0];
                        String rpid = str[1];
                        String action = str[2];
                        Integer category = tuple._2;
                        applyStat applyStat = new applyStat();
                        applyStat.setAction(action);
                        applyStat.setCategory(category);
                        applyStat.setCreated(time);
                        applyStat.setRpid(rpid);
                        applyStats.add(applyStat);
                    }
                    if (applyStats.size() > 0) {
                        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                        Connection conn = jdbcUtils.getConnection();
                        IApplyStatDAO applyStatDAO = DAOFactory.getApplyStatDAO();
                        applyStatDAO.updataBatch(applyStats, conn, 0);
                        System.out.println("mysql 2 applystat instance==> " + applyStats.size());
                        applyStats.clear();
                        if (conn != null) {
                            jdbcUtils.closeConnection(conn);
                        }
                    }
                }
            });
            numrdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                @Override
                public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                    List<applyStat> applyStats = new ArrayList<applyStat>();
                    Tuple2<String, Integer> tuple = null;
                    while (iterator.hasNext()) {
                        tuple = iterator.next();
                        String[] str = tuple._1.split("&");
                        String time = str[0];
                        String rpid = str[1];
                        String action = str[2];
                        System.out.print("");
                        Integer category = tuple._2;
                        applyStat applyStat = new applyStat();
                        applyStat.setAction(action);
                        applyStat.setCategory(category);
                        applyStat.setCreated(time);
                        applyStat.setRpid(rpid);
                        applyStats.add(applyStat);
                    }
                    if (applyStats.size() > 0) {
                        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                        Connection conn = jdbcUtils.getConnection();
                        IApplyStatDAO applyStatDAO = DAOFactory.getApplyStatDAO();
                        applyStatDAO.updataBatch(applyStats, conn, 3);
                        System.out.println("mysql 3 applystat num==> " + applyStats.size());
                        applyStats.clear();
                        if (conn != null) {
                            jdbcUtils.closeConnection(conn);
                        }
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获得当天时间戳 hbase rowkey
     *
     * @param date
     * @return
     */
    private static long getTimes(String date) {
        SimpleDateFormat day = new SimpleDateFormat("yyyy:MM:dd");
        Date parse = null;
        long l = 0l;
        try {
            parse = day.parse(date);
            l = parse.getTime() / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return l;
    }

    private static long getDays(String times) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            Long time = Long.parseLong(times);
            String d = format.format(time);
            date = format.parse(d);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime() / 1000;
    }
}
