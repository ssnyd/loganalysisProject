//package com.yonyou;
//
//import com.google.common.base.Optional;
//import com.yonyou.conf.ConfigurationManager;
//import com.yonyou.constant.Constants;
//import com.yonyou.dao.IMemIdStatDAO;
//import com.yonyou.dao.ILogStatDAO;
//import com.yonyou.dao.IUVStatDAO;
//import com.yonyou.dao.factory.DAOFactory;
//import com.yonyou.dao.IIPVStatDAO;
//import com.yonyou.entity.IPVStat;
//import com.yonyou.entity.MemIDStat;
//import com.yonyou.entity.UVStat;
//import com.yonyou.hbaseUtil.HbaseConnectionFactory;
//import com.yonyou.jdbc.JDBCHelper;
//import com.yonyou.jdbc.JDBCUtils;
//import com.yonyou.utils.HttpReqUtil;
//import com.yonyou.utils.JSONUtil;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.retry.RetryUntilElapsed;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.storage.StorageLevel;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.*;
//import org.apache.spark.streaming.kafka.HasOffsetRanges;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import kafka.serializer.StringDecoder;
//import org.apache.spark.streaming.kafka.OffsetRange;
//import scala.Tuple2;
//
//import java.sql.Connection;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicReference;
//
///**
// * Created by ChenXiaoLei on 2016/11/18.
// */
//public class SSODK {
//    public static JavaStreamingContext createContext(){
//        final Map<String, String> params = new HashMap<String, String>();
//        params.put("driverClassName", "com.mysql.jdbc.Driver");
//        params.put("url", "jdbc:mysql://192.168.1.151:3306/hive");
//        params.put("username", "hive");
//        params.put("password", "hive");
//
//        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("SparkTest");
//
//        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
//        jsc.checkpoint("/checkpoint");
//
//        Map<String, String> kafkaParams = new HashMap<String, String>();
//        kafkaParams.put("metadata.broker.list","lxjr-hadoop01:9092,lxjr-hadoop02:9092,lxjr-hadoop03:9092");
//
//        Set<String> topics = new HashSet<String>();
//        topics.add("esn_accesslog");
//
//        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jsc, String.class,
//                String.class, StringDecoder.class,
//                StringDecoder.class, kafkaParams,
//                topics);
//
//        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
//        JavaDStream<String> words = lines.transformToPair(
//                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
//                    @Override
//                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
//                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//                        offsetRanges.set(offsets);
//                        return rdd;
//                    }
//                }
//        ).flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
//            public Iterable<String> call(
//                    Tuple2<String, String> event)
//                    throws Exception {
//                String line = event._2;
//                return Arrays.asList(line);
//            }
//        });
//
//        JavaPairDStream<String, Integer> pairs = words
//                .mapToPair(new PairFunction<String, String, Integer>() {
//
//                    public Tuple2<String, Integer> call(
//                            String word) throws Exception {
//                        return new Tuple2<String, Integer>(
//                                word, 1);
//                    }
//                });
//
//        JavaPairDStream<String, Integer> wordsCount = pairs
//                .reduceByKey(new Function2<Integer, Integer, Integer>() {
//                    public Integer call(Integer v1, Integer v2)
//                            throws Exception {
//                        return v1 + v2;
//                    }
//                });
//////写到mysql
////        lines.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>(){
////            @Override
////            public void call(JavaPairRDD<String, String> t) throws Exception {
////                JDBCUtils jdbcUtils = JDBCUtils.getInstance();
////                Connection conn = jdbcUtils.getConnection();
////                for (OffsetRange offsetRange : offsetRanges.get()) {
////                    String sql = "update kafka_offsets set offset ='"
////                            + offsetRange.untilOffset() + "'  where topic='"
////                            + offsetRange.topic() + "' and partition='"
////                            + offsetRange.partition() + "'";
////                    JDBCHelper.executeUpdate(conn,sql,new Object[]{});
////                }
////                jdbcUtils.closeConnection(conn);
////            }
////
////        });
//        //写到zookeeper
//        lines.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>(){
//            @Override
//            public void call(JavaPairRDD<String, String> t) throws Exception {
//
//                ObjectMapper objectMapper = new ObjectMapper();
//
//                CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
//                        .connectString("lxjr-hadoop01:2181,lxjr-hadoop02:2181,lxjr-hadoop03:2181").connectionTimeoutMs(1000)
//                        .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
//                curatorFramework.start();
//                for (OffsetRange offsetRange : offsetRanges.get()) {
//                    final byte[] offsetBytes = objectMapper.writeValueAsBytes(offsetRange.untilOffset());
//                    String nodePath = "/consumers/spark-group/offsets/" + offsetRange.topic()+ "/" + offsetRange.partition();
//                    if(curatorFramework.checkExists().forPath(nodePath)!=null){
//                        curatorFramework.setData().forPath(nodePath,offsetBytes);
//                    }else{
//                        curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
//                    }
//                }
//                curatorFramework.close();
//            }
//        });
//
//        wordsCount.print();
//
//        return jsc;
//    }
//
//    public static void main(String[] args) {
//        JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
//            public JavaStreamingContext create() {
//                return createContext();
//            }
//        };
//
//        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate("/checkpoint", factory);
//        jsc.start();
//        jsc.awaitTermination();
//        jsc.close();
//    }
//
//
//
//
//}
