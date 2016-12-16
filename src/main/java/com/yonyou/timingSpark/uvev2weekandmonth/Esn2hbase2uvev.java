package com.yonyou.timingSpark.uvev2weekandmonth;

import com.yonyou.hbaseUtil.HbaseConnectionFactory;
import com.yonyou.utils.DateUtils;
import com.yonyou.utils.DateUtils2;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 读取hbase 每次一天 转换成 时间 memid ins_id 形式 字符串 按天存储
 * Created by chenxiaolei on 16/12/16.
 */
public class Esn2hbase2uvev {
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
        if (args.length == 2) {
            scan.setStartRow(Bytes.toBytes(args[0] + ":#"));
            scan.setStopRow(Bytes.toBytes(args[1] + "::"));
        } else {
            scan.setStartRow(Bytes.toBytes(DateUtils.getYesterdayDate() + ":#"));
            scan.setStopRow(Bytes.toBytes(DateUtils.getYesterdayDate() + "::"));
        }
        try {
            String tableName = "esn_accesslog";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                    sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class).repartition(200);
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
                    return v1 != null && v1.split("\t").length == 27;
                }
            });
            filter = filter.persist(StorageLevel.MEMORY_AND_DISK_SER());
            filter.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    String[] str = s.split("\t");
                    return str[26].split(":").length == 2 && str[25].split(":").length == 2 && str[23].split(":").length == 2 && (!"empty".equals(str[23].split(":")[1])) && str[24].split(":").length == 2;
                }
            }).map(new Function<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    String[] str = s.split("\t");
                    long time = DateUtils2.getTime(str[7], 1);
                    String dayTime = DateUtils2.getDayTime(str[7]);
                    String member_id = str[23].split(":")[1];
                    String instance_id = str[26].split(":")[1];
                    return dayTime + "&" + time + "&" + member_id + "&" + instance_id;
                }
            }).distinct().foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> iterator) throws Exception {
                    List<Put> puts = new ArrayList<Put>();
                    String s = "";
                    while (iterator.hasNext()) {
                        s = iterator.next();
                        if (!"".equals(s) && s.split("&").length == 4) {
                            String[] str = s.split("&");
                            Put put = new Put((String.valueOf(str[0])).getBytes());
                            put.addColumn(Bytes.toBytes("euv"), Bytes.toBytes("log"), Bytes.toBytes(str[1] + "&" + str[2] + "&" + str[3]));
                            puts.add(put);
                        }
                        if (puts.size() > 0) {
                            HTable hTable = HbaseConnectionFactory.gethTable("esn_evuv", "euv");
                            hTable.put(puts);
                            hTable.flushCommits();
                            System.out.println("hbase ==> " + puts.size());
                            puts.clear();
                            if (hTable != null) {
                                hTable.close();
                            }
                        }
                    }
                }
            });


            //
            //        .mapToPair(new PairFunction<String, String, String>() {
            //    @Override
            //    public Tuple2<String, String> call(String s) throws Exception {
            //        String[] str = s.split("\t");
            //        long time = DateUtils2.getTime(str[7], 1);
            //        String dayTime = DateUtils2.getDayTime(str[7]);
            //        String member_id =str[23].split(":")[1];
            //        String instance_id = str[26].split(":")[1];
            //        return new Tuple2<String, String>(dayTime,time+"&"+member_id+"&"+instance_id);
            //    }
            //});

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
