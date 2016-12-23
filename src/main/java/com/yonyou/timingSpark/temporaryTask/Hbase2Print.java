//package com.yonyou.timingSpark.TemporaryTask;
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
//import scala.Tuple2;
//
///**
// * Created by chenxiaolei on 16/12/13.
// */
//public class Hbase2Print {
//    public static void main(String[] args) {
//        SparkConf sconf = new SparkConf()
//                .setAppName("hbase2print")
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
//
//                    byte[] value = v1._2.getValue(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
//                    if (value != null) {
//                        return Bytes.toString(value);
//                    }
//                    return null;
//                }
//            });
//            filter.coalesce(1,true).saveAsTextFile("hdfs://cluster/txt/");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
