package com.yonyou.utils;

/**
 * Created by ChenXiaoLei on 2016/11/23.
 */
public class exampleTest {
    public static void main(String[] args) {
        System.out.println("hello world!");
    }
}
//package com.yonyou.newProjectApi;
//
//import com.yonyou.conf.ConfigurationManager;
//import com.yonyou.constant.Constants;
//import com.yonyou.utils.DateUtils;
//import com.yonyou.utils.JedisPoolUtils;
//import com.yonyou.utils.SparkUtils;
//import kafka.common.TopicAndPartition;
//import kafka.message.MessageAndMetadata;
//import kafka.serializer.StringDecoder;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.retry.RetryUntilElapsed;
//import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
//import org.apache.spark.streaming.kafka.HasOffsetRanges;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import org.apache.spark.streaming.kafka.OffsetRange;
//import redis.clients.jedis.Jedis;
//import scala.Tuple2;
//
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicReference;
//
///**
// * Created by ChenXiaoLei on 2016/11/23.
// */
//public class ESNStreaming2Hbase {
//    public static void main(String[] args) {
//        JavaStreamingContextFactory sparkfactory = new JavaStreamingContextFactory() {
//            public JavaStreamingContext create() {
//                return createContext();
//            }
//        };
//        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE), sparkfactory);
//        jssc.start();
//        jssc.awaitTermination();
//        jssc.close();
//    }
//    public static JavaStreamingContext createContext(){
//        SparkConf conf = new SparkConf()
//                .setAppName("ESNStreaming2Hbase")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .set("spark.streaming.blockInterval", "100")//ms→RDD
//                .set("spark.streaming.unpersist", "true")
//                .set("spark.shuffle.io.maxRetries", "60")
//                .set("spark.shuffle.io.retryWait", "60s")
//                .set("spark.reducer.maxSizeInFlight", "12");
////      conf.setMaster("local[2]");//本地测试
//        JavaStreamingContext jssc = new JavaStreamingContext(
//                conf, Durations.seconds(Integer.parseInt(ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE_TIME))));
//        //设置spark容错点
//        jssc.checkpoint(ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE));
//        Map<String, String> kafkaParams = new HashMap<String, String>();
//        kafkaParams.put("metadata.broker.list",
//                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
//        // 构建topic set
//        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
////        String[] kafkaTopicsSplited = kafkaTopics.split(",");
////        Set<String> topics = new HashSet<String>();
//
//        final Map<TopicAndPartition, Long> topicOffsets = SparkUtils.getTopicOffsets(ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST), ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE_TOPIC));
//
//        Map<TopicAndPartition, Long> consumerOffsets = SparkUtils.getConsumerOffsets(ConfigurationManager.getProperty(Constants.ZOOKEEPER_LIST), ConfigurationManager.getProperty(Constants.ESN_GROUPID), ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE_TOPIC));
//        if(null!=consumerOffsets && consumerOffsets.size()>0){
//            topicOffsets.putAll(consumerOffsets);
//        }
//
//        for(Map.Entry<TopicAndPartition, Long> item:topicOffsets.entrySet()){
//            item.setValue(item.getValue()-Long.parseLong(ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE_OFFSET_NUM)));
//        }
//        //打印topicoffset信息
//        for(Map.Entry<TopicAndPartition,Long> entry:topicOffsets.entrySet()){
//            System.out.println(entry.getKey().topic()+"\t"+entry.getKey().partition()+"\t"+entry.getValue());
//        }
//        JavaInputDStream<String> lines = KafkaUtils.createDirectStream(jssc,
//                String.class, String.class, StringDecoder.class,
//                StringDecoder.class, String.class, kafkaParams,
//                topicOffsets, new Function<MessageAndMetadata<String,String>,String>() {
//
//                    public String call(MessageAndMetadata<String, String> v1)
//                            throws Exception {
//                        return v1.message();
//                    }
//                });
//        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
//
//        JavaDStream<String> urls = lines.transform(
//                new Function<JavaRDD<String>, JavaRDD<String>>() {
//                    @Override
//                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
//                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//                        offsetRanges.set(offsets);
//                        return rdd;
//                    }
//                }
//        );
//        JavaDStream<String> filterDS = urls.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String v1) throws Exception {
//
//                return null;
//            }
//        });
//        //filter.mapToPair...
//
//        lines.foreachRDD(new VoidFunction<JavaRDD<String>>(){
//            @Override
//            public void call(JavaRDD<String> t) throws Exception {
//                ObjectMapper objectMapper = new ObjectMapper();
//                CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
//                        .connectString(ConfigurationManager.getProperty(Constants.ZOOKEEPER_LIST)).connectionTimeoutMs(1000)
//                        .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
//                curatorFramework.start();
//                for (OffsetRange offsetRange : offsetRanges.get()) {
//                    final byte[] offsetBytes = objectMapper.writeValueAsBytes(offsetRange.untilOffset());
//                    String nodePath = "/consumers/"+ ConfigurationManager.getProperty(Constants.ESN_GROUPID) +"/offsets/" + offsetRange.topic()+ "/" + offsetRange.partition();
//                    if(curatorFramework.checkExists().forPath(nodePath)!=null){
//                        curatorFramework.setData().forPath(nodePath,offsetBytes);
//                    }else{
//                        curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
//                    }
//                }
//                curatorFramework.close();
//            }
//        });
//        return jssc;
//    }
//}

