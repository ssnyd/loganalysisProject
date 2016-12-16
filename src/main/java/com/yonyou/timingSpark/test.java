//package com.yonyou.timingSpark;
//
//import com.yonyou.utils.DateUtils;
//import com.yonyou.utils.HttpReqUtil;
//import com.yonyou.utils.JSONUtil;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
//import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
//import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
//import org.apache.hadoop.hbase.util.Base64;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.storage.StorageLevel;
//import scala.Tuple2;
//
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.Iterator;
//import java.util.List;
//
///**
// * Created by chenxiaolei on 16/12/16.
// */
//public class test {
//    public static void main(String[] args) {
//        SparkConf sconf = new SparkConf()
//                .setAppName("applySpark")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        //      sconf.setMaster("local[2]");
//        JavaSparkContext sc = new JavaSparkContext(sconf);
//        Configuration conf = HBaseConfiguration.create();
//        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("app_case"));
//        scan.addColumn(Bytes.toBytes("app_case"), Bytes.toBytes("log"));
////      scan.setStartRow(Bytes.toBytes(getTimes("2016:11:28")+":#"));
////      scan.setStopRow(Bytes.toBytes(getTimes("2016:11:28")+"::"));
//        if (args.length == 2) {
//            scan.setStartRow(Bytes.toBytes(getTimes(args[0]) + ":#"));
//            scan.setStopRow(Bytes.toBytes(getTimes(args[1]) + "::"));
//        } else {
//            scan.setStartRow(Bytes.toBytes(getTimes(DateUtils.getYesterdayDate()) + ":#"));
//            scan.setStopRow(Bytes.toBytes(getTimes(DateUtils.getYesterdayDate()) + "::"));
//        }
//
//        try {
//            final String tableName = "esn_datacollection";
//            conf.set(TableInputFormat.INPUT_TABLE, tableName);
//            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
//            String ScanToString = Base64.encodeBytes(proto.toByteArray());
//            conf.set(TableInputFormat.SCAN, ScanToString);
//            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
//                    sc.newAPIHadoopRDD(conf, TableInputFormat.class,
//                            ImmutableBytesWritable.class, Result.class).repartition(200);
//            //读取的每一行数据
//            JavaRDD<String> filterRDD = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
//                @Override
//                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
//
//                    byte[] value = v1._2.getValue(Bytes.toBytes("app_case"), Bytes.toBytes("log"));
//                    if (value != null) {
//                        return Bytes.toString(value);
//                    }
//                    return null;
//                }
//            }).filter(new Function<String, Boolean>() {
//                @Override
//                public Boolean call(String v1) throws Exception {
//                    return v1 != null;
//                }
//            }).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
//                @Override
//                public Iterable<String> call(Iterator<String> iterator) throws Exception {
//                    List<String> list = new ArrayList<>();
//                    String line = "";
//                    while (iterator.hasNext()) {
//                        //action:view&app_id:22239&instance_id:3219&qz_id:3968&member_id:3469&mtime:1480044831884
//                        line = JSONUtil.getappId(iterator.next());
//                        list.add(line);
//                    }
//                    return list;
//                }
//            });
//            filterRDD = filterRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
//            JavaRDD<String> openIdRDD = filterRDD.filter(new Function<String, Boolean>() {
//                @Override
//                public Boolean call(String line) throws Exception {
//                    return line.split("&").length > 2 && line.split("&")[1].split(":").length == 2;
//                }
//            }).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
//                @Override
//                public Iterable<String> call(Iterator<String> iterator) throws Exception {
//                    List<String> list = new ArrayList<>();
//                    String app_id = "";
//                    String line = "";
//                    while (iterator.hasNext()) {
//                        line = iterator.next();
//                        app_id = line.split("&")[1].split(":")[1];
//                        if (app_id.contains("-")) {
//                            String[] str = line.split("&");
//                            line = "open_appid:"+app_id + "&" + "name:empty" + "&" + str[0] + "&" + "app_id:0" + "&" + str[2] + "&" + str[3] + "&" + str[4] + "&" + str[5];
//                        } else {
//                            String opid = JSONUtil.getopenId(HttpReqUtil.getResult("app/info/" + app_id, ""));
//                            line = opid + "&" + line;
//                        }
//                        //open_appid:110&name:协同日程新&action:view&app_id:22239&instance_id:3219&qz_id:3968&member_id:3469&mtime:1480044831884
//                        list.add(line);
//                    }
//                    return list;
//                }
//            }).filter(new Function<String, Boolean>() {
//                @Override
//                public Boolean call(String s) throws Exception {
//
//                    return s.split("&")[1].split(":").length == 2 && s.split("&")[0].split(":").length == 2;
//                }
//            }).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
//                @Override
//                public Iterable<String> call(Iterator<String> iterator) throws Exception {
//                    List list = new ArrayList();
//                    String app_id = null;
//                    String line = null;
//                    while (iterator.hasNext()) {
//                        line = iterator.next();
//                        String[] str = line.split("&");
//                        if (!"0".equals(str[0].split(":")[1])) {
//                            app_id = "app_id:0";
//                            line = str[0] + "&" + str[1] + "&" + str[2] + "&" + app_id + "&" + str[4] + "&" + str[5] + "&" + str[6] + "&" + str[7];
//                        }
//                        list.add(line);
//                    }
//                    return list;
//                }
//            });
//            openIdRDD.filter(new Function<String, Boolean>() {
//                @Override
//                public Boolean call(String s) throws Exception {
//                    return s.contains("add");
//                }
//            }).foreach(new VoidFunction<String>() {
//                @Override
//                public void call(String s) throws Exception {
//                    System.out.println(s);
//                }
//            });
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static long getTimes(String date) {
//        SimpleDateFormat day = new SimpleDateFormat("yyyy:MM:dd");
//        Date parse = null;
//        long l = 0l;
//        try {
//            parse = day.parse(date);
//            l = parse.getTime() / 1000;
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return l;
//    }
//
//    private static long getDays(String times) {
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//        Date date = null;
//        try {
//            Long time = Long.parseLong(times);
//            String d = format.format(time);
//            date = format.parse(d);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return date.getTime() / 1000;
//    }
//}
