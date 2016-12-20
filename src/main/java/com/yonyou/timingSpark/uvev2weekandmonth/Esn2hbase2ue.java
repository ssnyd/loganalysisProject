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
import scala.Tuple2;

import java.util.*;

/**
 * 为了以后计算大量uv方便
 * 获取esn_accesslog log 按照天去重 然后组装 时间＋memid＋instanceid 存到hbase
 * hbase存一份数据 最新的数据替换 故 也能达到去重的效果
 *
 *
 * Created by chenxiaolei on 16/12/19.
 */
public class Esn2hbase2ue {
    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("Esn2hbase2ue")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//      sconf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("accesslog"));
        scan.addColumn(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
        if (args.length == 2) {
            //默认是昨天的数据
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
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    String[] str = s.split("\t");
                    return str[26].split(":").length == 2 && str[23].split(":").length == 2 && (!"empty".equals(str[23].split(":")[1])) && (!"empty".equals(str[26].split(":")[1]));
                }
            }).map(new Function<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    String[] lines = s.split("\t");
                    return DateUtils2.getDayTime(lines[7]) + "\t" + lines[23].split(":")[1] + "\t" + lines[26].split(":")[1];
                }
            }).distinct();
//去重后直接存hbase
            filter.foreachPartition(new VoidFunction<Iterator<String>>() {
                @Override
                public void call(Iterator<String> iterator) throws Exception {
                    List<Put> puts = new ArrayList<Put>();
                    String s = "";
                    while (iterator.hasNext()) {
                        s = iterator.next();
                        String[] lines = s.split("\t");
                        if (lines.length == 3) {
                            Put put = new Put((String.valueOf(lines[0] + ":0" + UUID.randomUUID().toString().replace("-", "")).getBytes()));
                            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("log"), Bytes.toBytes(s));
                            puts.add(put);
                        }
                    }
                    if (puts.size() > 0) {
                        HTable hTable = HbaseConnectionFactory.gethTable("esn_uvev", "info");
                        hTable.put(puts);
                        hTable.flushCommits();
                        System.out.println("hbase ==> esn_uvev " + puts.size());
                        puts.clear();
                        if (hTable != null) {
                            hTable.close();
                        }
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
