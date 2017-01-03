package com.yonyou.timingSpark.uvev2weekandmonth;

import com.yonyou.dao.IUVStatDAO;
import com.yonyou.dao.factory.DAOFactory;
import com.yonyou.entity.UVStat;
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
import scala.Tuple2;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 周的ev uv 计算 hbase ➡️ spark ➡️ mysql
 * Created by chenxiaolei on 16/12/16.
 */
public class UVMonthSpark {

    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("uvmonthSpark")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//      sconf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("uv"));
        scan.addColumn(Bytes.toBytes("uv"), Bytes.toBytes("log"));
//      scan.setStartRow(Bytes.toBytes("2016:10:23:#"));
//      scan.setStopRow(Bytes.toBytes("2016:10:31::"));
        if(args.length==2){
            scan.setStartRow(Bytes.toBytes(DateUtils.getMonthTime(args[0])+":#"));
            scan.setStopRow(Bytes.toBytes(DateUtils.getMonthTime(args[1])+"::"));
        }else {
            scan.setStartRow(Bytes.toBytes(DateUtils.getMonthTime(DateUtils.getYesterdayDate())+":#"));
            scan.setStopRow(Bytes.toBytes(DateUtils.getMonthTime(DateUtils.getYesterdayDate())+"::"));
        }
        final Broadcast<String> broadcast = sc.broadcast(getKey(args));
        try {
            String tableName = "esn_month_uv";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                    sc.newAPIHadoopRDD(conf,  TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class);
            //读取的每一行数据
            JavaRDD<String> filter = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                @Override
                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {

                    byte[] value = v1._2.getValue(Bytes.toBytes("uv"), Bytes.toBytes("log"));
                    if (value != null) {
                        return Bytes.toString(value);
                    }
                    return null;
                }
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String v1) throws Exception {
                    return v1 != null && v1.split(":").length==2;
                }
            });
            filter.mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<String, Integer>(s.split(":")[0],1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1+v2;
                }
            },1).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                @Override
                public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                    List<UVStat> uvStats = new ArrayList<UVStat>();
                    Tuple2<String, Integer> tuple = null;
                    while (iterator.hasNext()) {
                        tuple = iterator.next();
                        String created = tuple._1;
                        String type = "thismonth";
                        String clientType = "all";
                        Integer num = tuple._2;
                        UVStat uvStat = new UVStat();
                        uvStat.setType(type);
                        uvStat.setClientType(clientType);
                        uvStat.setCreated(DateUtils.getTimestamp(broadcast.value()));
                        uvStat.setNum(num);
                        uvStats.add(uvStat);
                    }
                    if (uvStats.size()>0){
                        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                        Connection conn = jdbcUtils.getConnection();
                        IUVStatDAO uvStatDAO = DAOFactory.getUVStatDAO();
                        uvStatDAO.updataBatch(uvStats,conn);
                        System.out.println("mysql month uvstat ==> "+uvStats.size());
                        uvStats.clear();
                        if (conn != null) {
                            jdbcUtils.closeConnection(conn);
                        }
                    }
                }
            });

        }
        catch (Exception e) {
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
}
