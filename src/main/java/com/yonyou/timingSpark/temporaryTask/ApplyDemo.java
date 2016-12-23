//package com.yonyou.timingSpark;
//
//import com.yonyou.utils.DateUtils;
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
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
//import scala.Tuple2;
//
//import java.io.IOException;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//
///**
// * Created by chenxiaolei on 16/12/12.
// */
//public class ApplyDemo {
//    public static void main(String[] args) throws IOException {
//        SparkConf sconf = new SparkConf()
//                .setAppName("uvipvSpark")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
////      sconf.setMaster("local[2]");
//        JavaSparkContext sc = new JavaSparkContext(sconf);
//        Configuration conf = HBaseConfiguration.create();
//        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("accesslog"));
//        scan.addColumn(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
////      scan.setStartRow(Bytes.toBytes("2016:10:23:#"));
////      scan.setStopRow(Bytes.toBytes("2016:10:31::"));
//        if(args.length==2){
//            scan.setStartRow(Bytes.toBytes(args[0]+":#"));
//            scan.setStopRow(Bytes.toBytes(args[1]+"::"));
//            System.out.print("");
//        }else {
//            scan.setStartRow(Bytes.toBytes(DateUtils.getYesterdayDate()+":#"));
//            scan.setStopRow(Bytes.toBytes(DateUtils.getYesterdayDate()+"::"));
//        }
//            String tableName = "esn_accesslog";
//            conf.set(TableInputFormat.INPUT_TABLE, tableName);
//            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
//            String ScanToString = Base64.encodeBytes(proto.toByteArray());
//            conf.set(TableInputFormat.SCAN, ScanToString);
//            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
//                    sc.newAPIHadoopRDD(conf,  TableInputFormat.class,
//                            ImmutableBytesWritable.class, Result.class).repartition(1000);
//            //读取的每一行数据
//            JavaRDD<String> filter = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
//                @Override
//                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
//
//                    byte[] value = v1._2.getValue(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
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
//            });
//            filter.foreach(new VoidFunction<String>() {
//                @Override
//                public void call(String s) throws Exception {
//                    System.out.println(s);
//                }
//            });
//    }
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
//}
