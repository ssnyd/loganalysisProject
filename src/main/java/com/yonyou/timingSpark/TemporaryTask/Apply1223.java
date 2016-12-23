package com.yonyou.timingSpark.TemporaryTask;

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
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by chenxiaolei on 16/12/23.
 */
public class Apply1223 {
    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("applySpark")
                .set("spark.default.parallelism", "150")//並行度，reparation后生效(因为集群现在的配置是8核，按照每个核心有一个vcore，就是16，三个worker节点，就是16*3，并行度设置为3倍的话：16*3*3=144，故，这里设置150)
                .set("spark.locality.wait", "100ms")
                .set("spark.shuffle.manager", "hash")//使用hash的shufflemanager
                .set("spark.shuffle.consolidateFiles", "true")//shufflemap端开启合并较小落地文件（hashshufflemanager方式一个task对应一个文件，开启合并，reduce端有几个就是固定几个文件，提前分配好省着merge了）
                .set("spark.shuffle.file.buffer", "64")//shufflemap端mini环形缓冲区bucket的大小调大一倍，默认32KB
                .set("spark.reducer.maxSizeInFlight", "24")//从shufflemap端拉取数据24，默认48M
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//序列化
                .set("spark.shuffle.io.maxRetries", "10")//GC重试次数，默认3
                .set("spark.shuffle.io.retryWait", "30s");//GC等待时长，默认5s
//      sconf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("app_case"));
        scan.addColumn(Bytes.toBytes("app_case"), Bytes.toBytes("log"));
//      scan.setStartRow(Bytes.toBytes(getTimes("2016:11:28")+":#"));
//      scan.setStopRow(Bytes.toBytes(getTimes("2016:11:28")+"::"));
        if (args.length == 2) {
            scan.setStartRow(Bytes.toBytes(getTimes(args[0]) + ":#"));
            scan.setStopRow(Bytes.toBytes(getTimes(args[1]) + "::"));
        } else {
            scan.setStartRow(Bytes.toBytes(getTimes(DateUtils.getYesterdayDate()) + ":#"));
            scan.setStopRow(Bytes.toBytes(getTimes(DateUtils.getYesterdayDate()) + "::"));
        }

        try {
            final String tableName = "esn_datacollection";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                    sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class).repartition(200);
            //读取的每一行数据
            JavaRDD<String> filterRDD = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                @Override
                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {

                    byte[] value = v1._2.getValue(Bytes.toBytes("app_case"), Bytes.toBytes("log"));
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
            filterRDD.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String v1) throws Exception {

                    return v1.contains("\"member_id\":\"2877553\"")&&v1.contains("\"action\":\"view\"");
                }
            }).foreach(new VoidFunction<String>() {
                @Override
                public void call(String s) throws Exception {
                    System.out.println(s);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
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
