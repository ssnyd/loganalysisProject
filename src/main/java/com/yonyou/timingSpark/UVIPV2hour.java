package com.yonyou.timingSpark;
import com.alibaba.fastjson.JSONObject;
import com.yonyou.dao.IIPVStatDAO;
import com.yonyou.dao.IUVStatDAO;
import com.yonyou.dao.factory.DAOFactory;
import com.yonyou.entity.ApplysStat;
import com.yonyou.entity.IPVStat;
import com.yonyou.entity.UVStat;
import com.yonyou.hbaseUtil.HbaseConnectionFactory;
import com.yonyou.jdbc.JDBCUtils;
import com.yonyou.utils.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
/**
 * Created by ChenXiaoLei on 2016/11/28.
 */
public class UVIPV2hour {
    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("uvipv2hour")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//      sconf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("accesslog"));
        scan.addColumn(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
//      scan.setStartRow(Bytes.toBytes("2016:10:23:#"));
//      scan.setStopRow(Bytes.toBytes("2016:10:31::"));
        scan.setStartRow(Bytes.toBytes(DateUtils.getlasthourDate()+":#"));
        scan.setStopRow(Bytes.toBytes(DateUtils.getlasthourDate()+"::"));

        try {
            String tableName = "esn_accesslog";
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

                    byte[] value = v1._2.getValue(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
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
            });
            filter = filter.persist(StorageLevel.MEMORY_AND_DISK_SER());
            JavaPairRDD<String, Integer> totaluvRdd = calculateUVSta(filter);
            JavaPairRDD<String, String> totalipvRdd = calculateIPVSta(filter);
        }
        catch (Exception e) {
            e.printStackTrace();
        }


}
    private static JavaPairRDD<String, Integer> calculateUVSta(JavaRDD<String> line){
        JavaPairRDD<String, Integer> totalRDD = line.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.split("\t").length >= 26 && v1.split("\t")[24].split(":").length == 2;
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String log) throws Exception {
                String[] logSplited = log.split("\t");
                long timestamp = getTime(logSplited[7], 2);//获得每小时的时间戳
                String memberId = logSplited[23].split(":")[1];//获得memid
//                String memberId = new Random().nextInt(100)+1+"";//获得memid
//                String qzid = logSplited[24].split(":")[1];//获得qzid
                return new Tuple2<String, Integer>(timestamp + "&" + memberId, 1);
            }
        }).filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> tuple2) throws Exception {
                return !tuple2._1.split("&")[1].equals("empty");
            }
        });

        JavaPairRDD<String, Integer> hourRDD = totalRDD.distinct().mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2._1.split("&")[0]+"1hour", 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        hourRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Integer>> tuple2) throws Exception {
                List<UVStat> uvStats = new ArrayList<UVStat>();
                Tuple2<String, Integer> tuple = null;
                while (tuple2.hasNext()) {
                    tuple = tuple2.next();
                    String _created = tuple._1;
                    String created = _created.substring(0,_created.length()-5);
                    String type = "1hour";
                    if(_created.contains("1day")){
                        created = _created.substring(0,_created.length()-4);
                        type = "1day";
                        System.out.print("");
                    }
                    String clientType = "all";
                    Integer num = tuple._2;
                    UVStat uvStat = new UVStat();
                    uvStat.setType(type);
                    uvStat.setClientType(clientType);
                    uvStat.setCreated(created);
                    uvStat.setNum(num);
                    uvStats.add(uvStat);
                }
                if (uvStats.size()>0){
                    JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                    Connection conn = jdbcUtils.getConnection();
                    IUVStatDAO uvStatDAO = DAOFactory.getUVStatDAO();
                    uvStatDAO.updataBatch(uvStats,conn);
                    System.out.println("mysql 2 uvstat hour==> "+uvStats.size());
                    uvStats.clear();
                    if (conn != null) {
                        jdbcUtils.closeConnection(conn);
                    }
                }
            }
        });
        return totalRDD;
    }
    private static JavaPairRDD<String, String> calculateIPVSta(JavaRDD<String> line){
        JavaPairRDD<String, String> totalRDD = line.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String log) throws Exception {
                String[] logSplited = log.split("\t");
                long timestamp = getTime(logSplited[7], 2);//每小时的时间戳
                String ip = logSplited[0];
                return new Tuple2<String, String>(timestamp + "", ip);
            }
        });

        JavaPairRDD<String, Integer> hourRDD = totalRDD.distinct().mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2._1+"hour", 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        hourRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Integer>> tuple2) throws Exception {
                List<IPVStat> ipvStats = new ArrayList<IPVStat>();
                Tuple2<String, Integer> tuple = null;
                while (tuple2.hasNext()) {
                    tuple = tuple2.next();
                    String _created = tuple._1;
                    String created = _created.substring(0,_created.length()-4);
                    String type = "1hour";
                    if(_created.contains("day")){
                        created = _created.substring(0,_created.length()-3);
                        type = "1day";
                        System.out.print("");
                    }
                    String clientType = "all";
                    Integer num = tuple._2;
                    IPVStat ipvStat = new IPVStat();
                    ipvStat.setClientType(clientType);
                    ipvStat.setType(type);
                    ipvStat.setCreated(created);
                    ipvStat.setNum(num);
                    ipvStats.add(ipvStat);

                }
                if (ipvStats.size()>0){
                    JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                    Connection conn = jdbcUtils.getConnection();
                    IIPVStatDAO ipvStatDAO = DAOFactory.getIPVStatDAO();
                    ipvStatDAO.updataBatch(ipvStats,conn);
                    System.out.println("mysql 2 ipvstat hour==> "+ipvStats.size());
                    ipvStats.clear();
                    if (conn != null) {
                        jdbcUtils.closeConnection(conn);
                    }
                }
            }
        });
        return totalRDD;
    }
    private static long getTime(String timestamp , int num ) {
        String strDateTime = timestamp.replace("[", "").replace("]", "");
        long datekey = 0l;
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat day = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd HH");
        Date t = null;
        String format = "";
        try {
            t = formatter.parse(strDateTime);
            if (1 == num ){
                format = day.format(t);
                t = day.parse(format);
            } else if (2 == num) {
                format = hour.format(t);
                t = hour.parse(format);
            }
            datekey = t.getTime() / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return datekey;
    }
    private static long getTime(String timestamp) {
        String strDateTime = timestamp.replace("[", "").replace("]", "");
        long datekey = 0l;
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat day = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date t = null;
        String format = "";
        try {
            t = formatter.parse(strDateTime);
            format = day.format(t);
            t = day.parse(format);
            datekey = t.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return datekey;
    }
    private static long getDayLong(long time) {
        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd");
        long day =0l;
        try {
            String format = hour.format(time*1000);
            Date date = new Date(time * 1000);

            Date parse = hour.parse(format);
            day = parse.getTime() / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return day;
    }


}
