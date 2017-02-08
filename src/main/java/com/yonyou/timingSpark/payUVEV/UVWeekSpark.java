package com.yonyou.timingSpark.payUVEV;

import com.yonyou.dao.IPayEUUVDAO;
import com.yonyou.dao.factory.DAOFactory;
import com.yonyou.entity.enterprise.EVStat;
import com.yonyou.jdbc.JDBCUtils;
import com.yonyou.utils.DateUtils;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 周的uv 计算 hbase ➡️ spark ➡️ mysql
 * Created by chenxiaolei on 16/12/16.
 */
public class UVWeekSpark {

    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("payuvweekSpark")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//      sconf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        // 从mysql中查询付费企业，将其转换为一个rdd
        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        Connection conn = jdbcUtils.getConnection();
        IPayEUUVDAO payStatDAO = DAOFactory.getPayStatDAO();
        List<String> lists = null;
        if (args.length == 2){
            lists = payStatDAO.findAll(conn, getTimes(args[0]) + "", getTimes(args[0]) + "");
        }else {
            lists = payStatDAO.findAll(conn, getTimes(DateUtils.getYesterdayDate()) + "", getTimes(DateUtils.getYesterdayDate()) + "");
        }
        Map<String, Boolean> map = new HashMap<String, Boolean>();
        for (String s : lists) {
            map.put(s, true);
        }
        final Broadcast<Map<String, Boolean>> broad = sc.broadcast(map);

        if (conn != null) {
            jdbcUtils.closeConnection(conn);
        }
        ////////////////////////////////////////////////////////////////////
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("eu"));
        scan.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("log"));
//      scan.setStartRow(Bytes.toBytes("2016:10:23:#"));
//      scan.setStopRow(Bytes.toBytes("2016:10:31::"));
        if (args.length == 2) {
            scan.setStartRow(Bytes.toBytes(DateUtils.getWeekTime(args[0]) + ":#"));
            scan.setStopRow(Bytes.toBytes(DateUtils.getWeekTime(args[1]) + "::"));
        } else {
            scan.setStartRow(Bytes.toBytes(DateUtils.getWeekTime(DateUtils.getYesterdayDate()) + ":#"));
            scan.setStopRow(Bytes.toBytes(DateUtils.getWeekTime(DateUtils.getYesterdayDate()) + "::"));
        }
        final Broadcast<String> broadcast = sc.broadcast(getKey(args));
        try {
            String tableName = "esn_week_eu";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                    sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class);
            //读取的每一行数据 时间 企业id memID
            JavaRDD<String> filter = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                @Override
                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {

                    byte[] value = v1._2.getValue(Bytes.toBytes("eu"), Bytes.toBytes("log"));
                    if (value != null) {
                        return Bytes.toString(value);
                    }
                    return null;
                }
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String v1) throws Exception {
                    return v1 != null && v1.split(":").length == 3;
                }
            });
            JavaRDD<String> map2pay = filter.mapToPair(new PairFunction<String, String, String>() {
                @Override
                public Tuple2<String, String> call(String s) throws Exception {
                    String[] parm2 = s.split(":");
                    return new Tuple2<String, String>(parm2[1], s);
                }
            }).filter(new Function<Tuple2<String, String>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, String> v1) throws Exception {
                    Map<String, Boolean> value = broad.value();
                    if (value.get(v1._1) == null) {
                        return false;
                    }
                    return true;
                }
            }).map(new Function<Tuple2<String, String>, String>() {
                @Override
                public String call(Tuple2<String, String> v1) throws Exception {
                    String[] s = v1._2.split(":");
                    return s[0]+":"+s[2];
                }
            }).distinct();

            map2pay = map2pay.persist(StorageLevel.MEMORY_ONLY_SER());
            map2pay.mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<String, Integer>(s.split(":")[0], 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            }, 1).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                @Override
                public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                    List<EVStat> evStats = new ArrayList<EVStat>();
                    Tuple2<String, Integer> tuple2 = null;
                    while (iterator.hasNext()) {
                        tuple2 = iterator.next();
                        EVStat evStat = new EVStat();
                        evStat.setType("thisweek");
                        evStat.setCreated(DateUtils.getTimestamp(broadcast.value()));
                        evStat.setNum(tuple2._2);
                        evStats.add(evStat);
                    }
                    if (evStats.size() > 0) {
                        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                        Connection conn = jdbcUtils.getConnection();
                        IPayEUUVDAO payStatDAO = DAOFactory.getPayStatDAO();
                        payStatDAO.updataBatch(evStats,conn,1);
                        System.out.println("mysql week uvstat ==> " + evStats.size());
                        evStats.clear();
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

    private static String getKey(String[] s1) {
        if (s1.length == 2) {
            return DateUtils.parseDate(s1[0]);
        } else {
            return DateUtils.getYesterdayDate();
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
}
