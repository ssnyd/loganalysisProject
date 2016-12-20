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
public class Esn2hbase2monthuv {
    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("Esn2hbase2monthuv")
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

            //开始转换 ➡️ 月转换 uv
            JavaRDD<String> uvmonthrdd = filter.map(new Function<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    String[] lines = s.split("\t");
                    String key_date = DateUtils.getMonthTime(lines[0]);
                    String member_id = lines[1];
                    return key_date + "&" + member_id;
                }
            }).distinct();

//存uv month
            uvmonthrdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> iterator2) throws Exception {
                    List<Put> puts2 = new ArrayList<Put>();
                    String s2 = "";
                    while (iterator2.hasNext()) {
                        s2 = iterator2.next();
                        String[] lines2 = s2.split("&");
                        if (lines2.length == 2) {
                            Put put2 = new Put((String.valueOf(lines2[0] + ":" + lines2[1]).getBytes()));
                            put2.addColumn(Bytes.toBytes("uv"), Bytes.toBytes("log"), Bytes.toBytes(DateUtils.getTimestamp(lines2[0]) + ":" + lines2[1]));
                            puts2.add(put2);
                        }
                    }
                    if (puts2.size() > 0) {
                        HTable hTable2 = HbaseConnectionFactory.gethTable("esn_month_uv", "uv");
                        hTable2.put(puts2);
                        hTable2.flushCommits();
                        System.out.println("hbase ==> esn_month_uv " + puts2.size());
                        puts2.clear();
                        if (hTable2 != null) {
                            hTable2.close();
                        }
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
