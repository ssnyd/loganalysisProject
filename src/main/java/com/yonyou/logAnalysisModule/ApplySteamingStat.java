package com.yonyou.logAnalysisModule;

import com.yonyou.conf.ConfigurationManager;
import com.yonyou.constant.Constants;
import com.yonyou.utils.DateUtils;
import com.yonyou.utils.JedisPoolUtils;
import com.yonyou.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import com.google.common.collect.ImmutableMap;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndMetadata;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.kafka.OffsetRange;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
/**
 * Created by ChenXiaoLei on 2016/11/18.
 */
public class ApplySteamingStat {
    public static void main(String[] args) {
        JavaStreamingContextFactory sparkfactory = new JavaStreamingContextFactory() {
            public JavaStreamingContext create() {
                return createContext();
            }
        };
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate("hdfs://cluster/streaming/applyanaly", sparkfactory);
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
     public static JavaStreamingContext createContext(){
         SparkConf conf = new SparkConf()
                .setAppName("applyspark")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.streaming.blockInterval", "100")//ms→RDD
                .set("spark.streaming.unpersist", "true")
                .set("spark.shuffle.io.maxRetries", "60")
                .set("spark.shuffle.io.retryWait", "60s")
                .set("spark.reducer.maxSizeInFlight", "12")
                .set("spark.streaming.receiver.writeAheadLog.enable", "true");
//      conf.setMaster("local[2]");//本地测试
        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(30));
        //设置spark容错点
        jssc.checkpoint("hdfs://cluster/streaming/applyanaly");
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Set<String> topics = new HashSet<String>();

        final Map<TopicAndPartition, Long> topicOffsets = SparkUtils.getTopicOffsets("hdslave1:9092,hdslave2:9092,hdmaster:9092", "esn_accesslog");

        Map<TopicAndPartition, Long> consumerOffsets = SparkUtils.getConsumerOffsets("hdslave1:2181,hdslave2:2181,hdmaster:2181", "spark-group", "esn_accesslog");
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
                 String[] split = v1.split("\t");
                 return split.length == 20 && split[9].contains(".htm") && split[9].contains("r=");
             }
         });
         filter.mapToPair(new PairFunction<String, String, String>() {
             @Override
             public Tuple2<String, String> call(String s) throws Exception {
                 String[] lines = s.split("\t");
                 String timestamp = DateUtils.getTime(lines[7]);
                 String m = lines[3];
                 String r = "";
                 try {
                     if ((lines[9].split(" ").length >= 3) && (lines[9].contains("?")) && (lines[9].contains("r="))) {
                         URL aURL = null;
                         aURL = new URL("http://test.com" + lines[9].split(" ")[1]);
                         if ((!"".equals(aURL.getQuery())) && (aURL.getQuery().contains("r="))) {
                             String[] str = aURL.getQuery().split("&");
                             for (int i = 0; i < str.length; i++)
                                 if ("r".equals(str[i].split("=")[0])) {
                                     r = str[i].split("=")[1];
                                     break;
                                 }
                         }
                     }
                 } catch (MalformedURLException e) {
                     e.printStackTrace();
                 }
                 return new Tuple2<String, String>(timestamp + "&" + m, r);
             }
         }).filter(new Function<Tuple2<String, String>, Boolean>() {
             @Override
             public Boolean call(Tuple2<String, String> v1) throws Exception {
                 return !"".equals(v1._2);
             }
         }).mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
             @Override
             public Tuple2<String, Long> call(Tuple2<String, String> tuple2) throws Exception {
                 return new Tuple2<String, Long>(tuple2._1+"&"+tuple2._2,1l);
             }
         }).reduceByKey(new Function2<Long, Long, Long>() {
             @Override
             public Long call(Long v1, Long v2) throws Exception {
                 return v1+v2;
             }
         }).foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
             @Override
             public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                 rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                     @Override
                     public void call(Iterator<Tuple2<String, Long>> tuple) throws Exception {
                         Tuple2<String, Long> tuple2 =null;
                         Jedis jedis = null;
                         try {
                             jedis = JedisPoolUtils.getJedis();
                             while (tuple.hasNext()) {
                                 tuple2 = tuple.next();
                                 String time = tuple2._1.split("&")[0];
                                 String m = tuple2._1.split("&")[1];
                                 String r = tuple2._1.split("&")[2];
                                 Long num = tuple2._2;
                                 jedis.hincrBy("OPSYS:tuiguang_turntable:"+m+":"+time,r,num);
                                 System.out.println("OPSYS:tuiguang_turntable:"+m+":"+time+"&"+r+"&"+num);
                             }
                         } finally {
                             if (jedis != null) {
                                 jedis.close();
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

                CuratorFramework  curatorFramework = CuratorFrameworkFactory.builder()
                        .connectString("hdslave1:2181,hdslave2:2181,hdmaster:2181").connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
                curatorFramework.start();
                for (OffsetRange offsetRange : offsetRanges.get()) {
                    final byte[] offsetBytes = objectMapper.writeValueAsBytes(offsetRange.untilOffset());
                    String nodePath = "/consumers/spark-group/offsets/" + offsetRange.topic()+ "/" + offsetRange.partition();
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
