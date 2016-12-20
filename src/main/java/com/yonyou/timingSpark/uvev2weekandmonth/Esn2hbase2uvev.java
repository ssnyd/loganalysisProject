//package com.yonyou.timingSpark.uvev2weekandmonth;
//
//import com.yonyou.hbaseUtil.HbaseConnectionFactory;
//import com.yonyou.utils.DateUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;
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
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.storage.StorageLevel;
//import scala.Tuple2;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
///**
// * 读取hbase 每次一天 进行转换 成每周一次 的key存储
// * Created by chenxiaolei on 16/12/16.
// */
//public class Esn2hbase2uvev {
//    public static void main(String[] args) {
//        SparkConf sconf = new SparkConf()
//                .setAppName("Esn2hbase2uvev")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
////      sconf.setMaster("local[2]");
//        JavaSparkContext sc = new JavaSparkContext(sconf);
//        Configuration conf = HBaseConfiguration.create();
//        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("accesslog"));
//        scan.addColumn(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
//        if (args.length == 2) {
//            scan.setStartRow(Bytes.toBytes(args[0] + ":#"));
//            scan.setStopRow(Bytes.toBytes(args[1] + "::"));
//        } else {
//            scan.setStartRow(Bytes.toBytes(DateUtils.getYesterdayDate() + ":#"));
//            scan.setStopRow(Bytes.toBytes(DateUtils.getYesterdayDate() + "::"));
//        }
//        try {
//            String tableName = "esn_accesslog";
//            conf.set(TableInputFormat.INPUT_TABLE, tableName);
//            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
//            String ScanToString = Base64.encodeBytes(proto.toByteArray());
//            conf.set(TableInputFormat.SCAN, ScanToString);
//            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
//                    sc.newAPIHadoopRDD(conf, TableInputFormat.class,
//                            ImmutableBytesWritable.class, Result.class).repartition(200);
//            //读取的每一行数据
//            JavaRDD<String> filter = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
//                @Override
//                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
//                    byte[] value = v1._2.getValue(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
//                    if (value != null) {
//                        return Bytes.toString(value);
//                    }
//                    return null;
//                }
//            }).filter(new Function<String, Boolean>() {
//                @Override
//                public Boolean call(String v1) throws Exception {
//                    return v1 != null && v1.split("\t").length == 27;
//                }
//            }).filter(new Function<String, Boolean>() {
//                @Override
//                public Boolean call(String s) throws Exception {
//                    String[] str = s.split("\t");
//                    return str[26].split(":").length == 2 && str[23].split(":").length == 2 && (!"empty".equals(str[23].split(":")[1])) && (!"empty".equals(str[26].split(":")[1]));
//                }
//            }).map(new Function<String, String>() {
//                @Override
//                public String call(String s) throws Exception {
//                    String[] lines = s.split("\t");
//                    return lines[7] + "\t" + lines[23].split(":")[1] + "\t" + lines[26].split(":")[1];
//                }
//            });
//            filter = filter.persist(StorageLevel.MEMORY_AND_DISK_SER());
//
//            //开始转换 ➡️ 周转换 uv
//            JavaRDD<String> uvweekrdd = filter.map(new Function<String, String>() {
//                @Override
//                public String call(String s) throws Exception {
//                    String[] lines = s.split("\t");
//                    String key_date = DateUtils.getWeekTime(lines[0]);
//                    String member_id = lines[1];
//                    return key_date + "&" + member_id;
//                }
//            }).distinct();
//            //开始转换 ➡️ 周转换 ev
//            JavaRDD<String> evweekrdd = filter.map(new Function<String, String>() {
//                @Override
//                public String call(String s) throws Exception {
//                    String[] lines = s.split("\t");
//                    String key_date = DateUtils.getWeekTime(lines[0]);
//                    String instance_id = lines[2];
//                    return key_date + "&" + instance_id;
//                }
//            }).distinct();
//            //开始转换 ➡️ 月转换 ev
//            JavaRDD<String> evmonthrdd = filter.map(new Function<String, String>() {
//                @Override
//                public String call(String s) throws Exception {
//                    String[] lines = s.split("\t");
//                    String key_date = DateUtils.getMonthTime(lines[0]);
//                    String instance_id = lines[2];
//                    return key_date + "&" + instance_id;
//                }
//            }).distinct();
//            //开始转换 ➡️ 月转换 uv
//            JavaRDD<String> uvmonthrdd = filter.map(new Function<String, String>() {
//                @Override
//                public String call(String s) throws Exception {
//                    String[] lines = s.split("\t");
//                    String key_date = DateUtils.getMonthTime(lines[0]);
//                    String member_id = lines[1];
//                    return key_date + "&" + member_id;
//                }
//            }).distinct();
//            uvweekrdd.foreachPartition(new VoidFunction<Iterator<String>>() {
//                @Override
//                public void call(Iterator<String> iterator1) throws Exception {
//                    List<Put> puts1 = new ArrayList<Put>();
//                    String s1 = "";
//                    while (iterator1.hasNext()) {
//                        s1 = iterator1.next();
//                        String[] lines1 = s1.split("&");
//                        if (lines1.length == 2) {
//                            Put put1 = new Put((String.valueOf(lines1[0] + ":" + lines1[1]).getBytes()));
//                            put1.addColumn(Bytes.toBytes("uv"), Bytes.toBytes("log"), Bytes.toBytes(DateUtils.getTimestamp(lines1[0]) + ":" + lines1[1]));
//                            puts1.add(put1);
//                        }
//                        if (puts1.size() > 0) {
//                            HTable hTable1 = HbaseConnectionFactory.gethTable("esn_week_uv", "uv");
//                            hTable1.put(puts1);
//                            hTable1.flushCommits();
//                            System.out.println("hbase ==> esn_week_uv " + puts1.size());
//                            puts1.clear();
//                            if (hTable1 != null) {
//                                hTable1.close();
//                            }
//                        }
//                    }
//                }
//            });
//            //存uv month
//            uvmonthrdd.foreachPartition(new VoidFunction<Iterator<String>>() {
//                @Override
//                public void call(Iterator<String> iterator2) throws Exception {
//                    List<Put> puts2 = new ArrayList<Put>();
//                    String s2 = "";
//                    while (iterator2.hasNext()) {
//                        s2 = iterator2.next();
//                        String[] lines2 = s2.split("&");
//                        if (lines2.length == 2) {
//                            Put put2 = new Put((String.valueOf(lines2[0] + ":" + lines2[1]).getBytes()));
//                            put2.addColumn(Bytes.toBytes("uv"), Bytes.toBytes("log"), Bytes.toBytes(DateUtils.getTimestamp(lines2[0]) + ":" + lines2[1]));
//                            puts2.add(put2);
//                        }
//                        if (puts2.size() > 0) {
//                            HTable hTable2 = HbaseConnectionFactory.gethTable("esn_month_uv", "uv");
//                            hTable2.put(puts2);
//                            hTable2.flushCommits();
//                            System.out.println("hbase ==> esn_month_uv " + puts2.size());
//                            puts2.clear();
//                            if (hTable2 != null) {
//                                hTable2.close();
//                            }
//                        }
//                    }
//                }
//            });
//            //存ev week
//            evweekrdd.foreachPartition(new VoidFunction<Iterator<String>>() {
//                @Override
//                public void call(Iterator<String> iterator3) throws Exception {
//                    List<Put> puts3 = new ArrayList<Put>();
//                    String s3 = "";
//                    while (iterator3.hasNext()) {
//                        s3 = iterator3.next();
//                        String[] lines3 = s3.split("&");
//                        if (lines3.length == 2) {
//                            Put put3 = new Put((String.valueOf(lines3[0] + ":" + lines3[1]).getBytes()));
//                            put3.addColumn(Bytes.toBytes("ev"), Bytes.toBytes("log"), Bytes.toBytes(DateUtils.getTimestamp(lines3[0]) + ":" + lines3[1]));
//                            puts3.add(put3);
//                        }
//                        if (puts3.size() > 0) {
//                            HTable hTable3 = HbaseConnectionFactory.gethTable("esn_week_ev", "ev");
//                            hTable3.put(puts3);
//                            hTable3.flushCommits();
//                            System.out.println("hbase ==> esn_week_ev " + puts3.size());
//                            puts3.clear();
//                            if (hTable3 != null) {
//                                hTable3.close();
//                            }
//                        }
//                    }
//                }
//            });
//            //存ev month
//            evmonthrdd.foreachPartition(new VoidFunction<Iterator<String>>() {
//                @Override
//                public void call(Iterator<String> iterator) throws Exception {
//                    List<Put> puts = new ArrayList<Put>();
//                    String s = "";
//                    while (iterator.hasNext()) {
//                        s = iterator.next();
//                        String[] lines = s.split("&");
//                        if (lines.length == 2) {
//                            Put put = new Put((String.valueOf(lines[0] + ":" + lines[1]).getBytes()));
//                            put.addColumn(Bytes.toBytes("ev"), Bytes.toBytes("log"), Bytes.toBytes(DateUtils.getTimestamp(lines[0]) + ":" + lines[1]));
//                            puts.add(put);
//                        }
//                        if (puts.size() > 0) {
//                            HTable hTable = HbaseConnectionFactory.gethTable("esn_month_ev", "ev");
//                            hTable.put(puts);
//                            hTable.flushCommits();
//                            System.out.println("hbase ==> esn_month_ev " + puts.size());
//                            puts.clear();
//                            if (hTable != null) {
//                                hTable.close();
//                            }
//                        }
//                    }
//                }
//            });
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
