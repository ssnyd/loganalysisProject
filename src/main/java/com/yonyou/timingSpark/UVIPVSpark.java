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
 * Created by ChenXiaoLei on 2016/11/22.
 */
public class UVIPVSpark {
    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("uvipvSpark")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//      sconf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("accesslog"));
        scan.addColumn(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
//      scan.setStartRow(Bytes.toBytes("2016:10:23:#"));
//      scan.setStopRow(Bytes.toBytes("2016:10:31::"));
        if(args.length==2){
            scan.setStartRow(Bytes.toBytes(args[0]+":#"));
            scan.setStopRow(Bytes.toBytes(args[1]+"::"));
            System.out.print("");
        }else {
            scan.setStartRow(Bytes.toBytes(DateUtils.getYesterdayDate()+":#"));
            scan.setStopRow(Bytes.toBytes(DateUtils.getYesterdayDate()+"::"));
        }
        try {
            String tableName = "esn_accesslog";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                    sc.newAPIHadoopRDD(conf,  TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class).repartition(1000);
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
            JavaPairRDD<String, String> apply2hbase =apply2Hbase(filter);
            JavaPairRDD<String, Integer> totaluvRdd = calculateUVSta(filter);
            JavaPairRDD<String, String> totalipvRdd = calculateIPVSta(filter);
        }
        catch (Exception e) {
            e.printStackTrace();
        }


}

    /**
     * 将应用类型的数据存到hbase
     * @param line
     * @return
     */

    private static JavaPairRDD<String,String> apply2Hbase(JavaRDD<String> line) {
        line.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] str = v1.split("\t");
                return str.length == 27 && str[26].split(":").length == 2 && str[25].split(":").length == 2 && str[23].split(":").length == 2 && (!"empty".equals(str[23].split(":")[1])) && str[24].split(":").length == 2;
            }
        }).mapPartitions(new FlatMapFunction<Iterator<String>, ApplysStat>() {
            @Override
            public Iterable<ApplysStat> call(Iterator<String> iterator) throws Exception {
                List<ApplysStat> applysStats = new ArrayList<>();
                String tuple = null;
                while (iterator.hasNext()) {
                    tuple = iterator.next();
                    String[] str = tuple.split("\t");
                    String action = "_click";
                    String client_ip = str[0];
                    String client = getClient(str[15]);
                    String device_model = "";
                    String device_name = "";
                    String member_id =str[23].split(":")[1];
                    String qz_id = str[24].split(":")[1];
                    String user_id = str[25].split(":")[1];
                    String instance_id = str[26].split(":")[1];
                    String ver_code = "";
                    String app_id = isApply2id(str[9]);
                    String mtime = getTime(str[7])+"";
                    ApplysStat applysStat = new ApplysStat();
                    applysStat.setAction(action);
                    applysStat.setApp_id(app_id);
                    applysStat.setClient(client);
                    applysStat.setClient_ip(client_ip);
                    applysStat.setDevice_model(device_model);
                    applysStat.setDevice_name(device_name);
                    applysStat.setInstance_id(instance_id);
                    applysStat.setMember_id(member_id);
                    applysStat.setMtime(mtime);
                    applysStat.setQz_id(qz_id);
                    applysStat.setUser_id(user_id);
                    applysStat.setVer_code(ver_code);
                    applysStats.add(applysStat);
                }
                return applysStats;
            }
        }).filter(new Function<ApplysStat, Boolean>() {
            @Override
            public Boolean call(ApplysStat v1) throws Exception {
                return !"".equals(v1.getApp_id());
            }
        }).mapToPair(new PairFunction<ApplysStat, String, String>() {
            @Override
            public Tuple2<String, String> call(ApplysStat as) throws Exception {
                String json = "{\"action\":\""+as.getAction()+"\",\"app_id\":\""+as.getApp_id()+"\",\"client\":\""+as.getClient()+"\",\"client_ip\":\""+as.getClient_ip()+"\",\"device_model\":\""+as.getDevice_model()+"\",\"device_name\":\""+as.getDevice_name()+"\",\"member_id\":\""+as.getMember_id()+"\",\"mtime\":\""+as.getMtime()+"\",\"qz_id\":\""+as.getQz_id()+"\",\"user_id\":\""+as.getUser_id()+"\",\"ver_code\":\""+as.getVer_code()+"\",\"instance_id\":\""+as.getInstance_id()+"\"}";
                JSONObject jsonObject = JSONObject.parseObject(json);
                Long mtime = jsonObject.getLong("mtime");
                long time = DateUtils.timeStamp2Date(mtime, null);
                return new Tuple2<String, String>(time+":"+UUID.randomUUID().toString().replace("-",""),json);
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
                Tuple2<String, String> tuple = null;
                List<Put> puts = new ArrayList<Put>();
                while (iterator.hasNext()) {
                    tuple = iterator.next();
                    if(tuple!=null){
                        Put put = new Put((String.valueOf(tuple._1)).getBytes());
                        put.addColumn(Bytes.toBytes("app_case"),Bytes.toBytes("log"),Bytes.toBytes(tuple._2));
                        puts.add(put);
                        System.out.print("");
                    }
                }
                if (puts.size()>0){
                    HTable hTable = HbaseConnectionFactory.gethTable("esn_datacollection", "app_case");
                    hTable.put(puts);
                    hTable.flushCommits();
                    System.out.println("hbase ==> "+puts.size());
                    puts.clear();
                    if (hTable!=null){
                        System.out.print("");
                        hTable.close();
                    }
                }
            }
        });
        return null;
    }
    private static String getClient(String str) {
        String client = "";
        if (str != null) {
            if (str.contains("Android")){
                client = "android";
            }else if (str.contains("iPhone")){
                client = "ios";
            }else {
                client = "";
            }
        }
        return client;
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

        JavaPairRDD<String, Integer> dayRDD = totalRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
                String[] s = tuple2._1.split("&");
                long hour = Long.parseLong(s[0]);
                long dayTime = getDayLong(hour);
                return new Tuple2<String, Integer>(dayTime + "&" + s[1], 1);
            }
        }).distinct().mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2._1.split("&")[0]+"1day", 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        dayRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
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
                    System.out.println("mysql 2 uvstat ==> "+uvStats.size());
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
        JavaPairRDD<String, Integer> dayRDD = totalRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                String time = tuple2._1;
                long hour = Long.parseLong(time);
                long dayTime = getDayLong(hour);
                return new Tuple2<String, String>(dayTime+"", tuple2._2);
            }
        }).distinct().mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2._1 + "day", 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        dayRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
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
                    System.out.println("mysql 2 ipvstat ==> "+ipvStats.size());
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
    private static String isApply2id(String request){
        String app_id = "";
        if ("".equals(request) || request == null){
            app_id = "";
        }else {
            if (request.contains("/rest/announce/getAnnounceList")){
                app_id = "85";
            }else if (request.contains("/rest/message/folderNum")){
                app_id = "55";
            }else if (request.contains("/rest/schedule/getUnreadList")){
                app_id = "25";
            }else if (request.contains("/rest/doc/index")){
                app_id = "50";
            }else if (request.contains("/rest/talk/getRecentTalkRecord")){
                app_id = "185";
            }else if (request.contains("/rest/toutiao/getSubClasses")){
                app_id = "165";
            }else if (request.contains("/rest/Yyfax/getYyfax.json")){
                app_id = "900";
            }else if (request.contains("/rest/app/appOauth")){
                if ((request.split(" ").length >= 3) && (request.contains("?")) && (request.contains("app_id"))) {
                    URL aURL = null;
                    try {
                        aURL = new URL("http://test.com" + request.split(" ")[1]);
                        if ((!"".equals(aURL.getQuery())) && (aURL.getQuery().contains("app_id"))) {
                            String[] strs = aURL.getQuery().split("&");
                            for (int i = 0; i < strs.length; i++)
                                if ("app_id".equals(strs[i].split("=")[0]) && strs[i].split("=").length==2) {
                                    app_id = strs[i].split("=")[1];
                                    break;
                                }
                        }
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return app_id;
    }





}
