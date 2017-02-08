package com.yonyou.timingSpark.uvev2weekandmonth;

import com.yonyou.hbaseUtil.HbaseConnectionFactory;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chenxiaolei on 16/12/19.
 */
public class Esn2hbase2weekeu {
    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("Esn2hbase2weekeu")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//      sconf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("log"));
        if (args.length == 2) {
            scan.setStartRow(Bytes.toBytes(args[0] + ":#"));
            scan.setStopRow(Bytes.toBytes(args[1] + "::"));
        } else {
            scan.setStartRow(Bytes.toBytes(DateUtils.getYesterdayDate() + ":#"));
            scan.setStopRow(Bytes.toBytes(DateUtils.getYesterdayDate() + "::"));
        }
        try {
            String tableName = "esn_uvev";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                    sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class);
            //读取的每一行数据
            JavaRDD<String> filter = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                @Override
                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                    byte[] value = v1._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("log"));
                    if (value != null) {
                        return Bytes.toString(value);
                    }
                    return null;
                }
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String v1) throws Exception {
                    return v1 != null && v1.split("\t").length == 3;
                }
            });
            filter = filter.persist(StorageLevel.MEMORY_AND_DISK_SER());

            //开始转换 ➡️ 周转换 uv
            JavaRDD<String> uvweekrdd = filter.map(new Function<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    String[] lines = s.split("\t");
                    String key_date = DateUtils.getWeekTime(lines[0]);
                    String member_id = lines[1];
                    String ins_id = lines[2];
                    return key_date + "&" +ins_id+"&"+ member_id;
                }
            }).distinct();
            uvweekrdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> iterator1) throws Exception {
                    List<Put> puts1 = new ArrayList<Put>();
                    String s1 = "";
                    while (iterator1.hasNext()) {
                        s1 = iterator1.next();
                        String[] lines1 = s1.split("&");
                        if (lines1.length == 3) {
                            Put put1 = new Put((String.valueOf(lines1[0] + ":" + lines1[1]+":"+lines1[2]).getBytes()));
                            put1.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("log"), Bytes.toBytes(DateUtils.getTimestamp(lines1[0]) + ":" + lines1[1]+":"+lines1[2]));
                            puts1.add(put1);
                        }
                    }
                    if (puts1.size() > 0) {
                        HTable hTable1 = HbaseConnectionFactory.gethTable("esn_week_eu", "eu");
                        hTable1.put(puts1);
                        hTable1.flushCommits();
                        System.out.println("hbase ==> esn_week_eu " + puts1.size());
                        puts1.clear();
                        if (hTable1 != null) {
                            hTable1.close();
                        }
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
