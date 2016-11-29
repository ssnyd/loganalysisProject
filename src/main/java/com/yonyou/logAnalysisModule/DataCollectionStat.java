package com.yonyou.logAnalysisModule;

import com.alibaba.fastjson.JSONObject;
import com.yonyou.conf.ConfigurationManager;
import com.yonyou.constant.Constants;
import com.yonyou.hbaseUtil.HbaseConnectionFactory;
import com.yonyou.utils.DateUtils;
import com.yonyou.utils.JedisPoolUtils;
import com.yonyou.utils.SparkUtils;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by ChenXiaoLei on 2016/11/21.
 */
public class DataCollectionStat {
    public static void main(String[] args) {
        JavaStreamingContextFactory sparkfactory = new JavaStreamingContextFactory() {
            public JavaStreamingContext create() {
                return createContext();
            }
        };
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate("hdfs://cluster/streaming/dataCollection", sparkfactory);
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
    public static JavaStreamingContext createContext(){
        SparkConf conf = new SparkConf()
                .setAppName("dataCollection")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.streaming.blockInterval", "100")//ms→RDD
                .set("spark.streaming.unpersist", "true")
                .set("spark.shuffle.io.maxRetries", "60")
                .set("spark.shuffle.io.retryWait", "60s")
                .set("spark.reducer.maxSizeInFlight", "12")
                .set("spark.streaming.receiver.writeAheadLog.enable", "true");
//      conf.setMaster("local[2]");//本地测试
        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(5));
        //设置spark容错点
        jssc.checkpoint("hdfs://cluster/streaming/dataCollection");
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Set<String> topics = new HashSet<String>();

        final Map<TopicAndPartition, Long> topicOffsets = SparkUtils.getTopicOffsets("hdslave1:9092,hdslave2:9092,hdmaster:9092", "esn_datacollection");

        Map<TopicAndPartition, Long> consumerOffsets = SparkUtils.getConsumerOffsets("hdslave1:2181,hdslave2:2181,hdmaster:2181", "spark-group1", "esn_datacollection");
        if(null!=consumerOffsets && consumerOffsets.size()>0){
            topicOffsets.putAll(consumerOffsets);
        }

        for(Map.Entry<TopicAndPartition, Long> item:topicOffsets.entrySet()){
            item.setValue(item.getValue()-0l);
        }
      for(Map.Entry<TopicAndPartition,Long> entry:topicOffsets.entrySet()){
          System.out.println(entry.getKey().topic()+"\t"+entry.getKey().partition()+"\t"+entry.getValue());
      }
         JavaInputDStream<String> lines = KafkaUtils.createDirectStream(jssc,
                 String.class, String.class, StringDecoder.class,
                 StringDecoder.class, String.class, kafkaParams,
                 topicOffsets, new Function<MessageAndMetadata<String,String>,String>() {

                     public String call(MessageAndMetadata<String, String> v1)
                             throws Exception {
                         return v1.message();
                     }
                 });
          final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        JavaDStream<String> urls = lines.transform(
                new Function<JavaRDD<String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                      OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                      offsetRanges.set(offsets);
                      return rdd;
                    }
                  }
                );
         JavaDStream<String> filter = urls.filter(new Function<String, Boolean>() {
             @Override
             public Boolean call(String v1) throws Exception {
                 return v1.contains("}")&&v1.contains("{")&&v1.contains("mtime");
             }
         });
        filter.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String tuple) throws Exception {
                    JSONObject jsonObject = JSONObject.parseObject(tuple);
                    Long mtime = jsonObject.getLong("mtime");
                    long time = DateUtils.timeStamp2Date(mtime, null);
                return new Tuple2<String, String>(time+":"+UUID.randomUUID().toString().replace("-",""),tuple);
            }
        }).foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
                        Tuple2<String, String> tuple = null;
                        List<Put> puts = new ArrayList<Put>();
                        while (iterator.hasNext()) {
                            tuple = iterator.next();
                            if(tuple!=null){
                                Put put = new Put((String.valueOf(tuple._1)).getBytes());
                                put.addColumn(Bytes.toBytes("app_case"),Bytes.toBytes("log"),Bytes.toBytes(tuple._2));
                                puts.add(put);
                            }
                        }
                        if (puts.size()>0){
                            HTable hTable = HbaseConnectionFactory.gethTable("esn_datacollection", "app_case");
                            hTable.put(puts);
                            hTable.flushCommits();
                            System.out.println("hbase ==> "+puts.size());
                            puts.clear();
                            if (hTable!=null){
                                hTable.close();
                            }
                        }
                    }
                });
            }
        });


         lines.foreachRDD(new VoidFunction<JavaRDD<String>>(){
            @Override
            public void call(JavaRDD<String> t) throws Exception {

                ObjectMapper objectMapper = new ObjectMapper();

                CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                        .connectString("hdslave1:2181,hdslave2:2181,hdmaster:2181").connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
                curatorFramework.start();
                for (OffsetRange offsetRange : offsetRanges.get()) {
                    final byte[] offsetBytes = objectMapper.writeValueAsBytes(offsetRange.untilOffset());
                    String nodePath = "/consumers/spark-group1/offsets/" + offsetRange.topic()+ "/" + offsetRange.partition();
                    if(curatorFramework.checkExists().forPath(nodePath)!=null){
                            curatorFramework.setData().forPath(nodePath,offsetBytes);
                        }else{
                            curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
                        }
                }
                curatorFramework.close();
            }
        });
         return jssc;
     }




}
