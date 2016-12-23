//package com.yonyou.myTest;
//import com.google.common.collect.ImmutableMap;
//import kafka.api.PartitionOffsetRequestInfo;
//import kafka.cluster.Broker;
//import kafka.common.TopicAndPartition;
//import kafka.javaapi.*;
//import kafka.javaapi.consumer.SimpleConsumer;
//import kafka.message.MessageAndMetadata;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.retry.RetryUntilElapsed;
//import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.*;
//import org.apache.spark.streaming.kafka.HasOffsetRanges;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import kafka.serializer.StringDecoder;
//import org.apache.spark.streaming.kafka.OffsetRange;
//import scala.Tuple2;
//
//import java.util.*;
//import java.util.concurrent.atomic.AtomicReference;
///**
// * Created by ChenXiaoLei on 2016/11/18.
// */
//public class SparkTest {
//    public static JavaStreamingContext createContext(){
//
//        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("SparkTest");
//
//        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));
//        jsc.checkpoint("/c");
//
//        Map<String, String> kafkaParams = new HashMap<String, String>();
//        kafkaParams.put("metadata.broker.list","lxjr-hadoop01:9092,lxjr-hadoop02:9092,lxjr-hadoop03:9092");
//
//        Map<TopicAndPartition, Long> topicOffsets = getTopicOffsets("lxjr-hadoop01:9092,lxjr-hadoop02:9092,lxjr-hadoop03:9092", "esn_accesslog");
//
//        Map<TopicAndPartition, Long> consumerOffsets = getConsumerOffsets("lxjr-hadoop01:2181,lxjr-hadoop02:2181,lxjr-hadoop03:2181", "spark-group", "esn_accesslog");
//        if(null!=consumerOffsets && consumerOffsets.size()>0){
//            topicOffsets.putAll(consumerOffsets);
//        }
//
//        for(Map.Entry<TopicAndPartition, Long> item:topicOffsets.entrySet()){
//            item.setValue(0l);
//        }
//      for(Map.Entry<TopicAndPartition,Long> entry:topicOffsets.entrySet()){
//          System.out.println(entry.getKey().topic()+"\t"+entry.getKey().partition()+"\t"+entry.getValue());
//      }
//
//        JavaInputDStream<String> lines = KafkaUtils.createDirectStream(jsc,
//                String.class, String.class, StringDecoder.class,
//                StringDecoder.class, String.class, kafkaParams,
//                topicOffsets, new Function<MessageAndMetadata<String,String>,String>() {
//
//                    public String call(MessageAndMetadata<String, String> v1)
//                            throws Exception {
//                        return v1.message();
//                    }
//                });
//
//
//
//        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
//
//        JavaDStream<String> words = lines.transform(
//                new Function<JavaRDD<String>, JavaRDD<String>>() {
//                    @Override
//                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
//                      OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//                      offsetRanges.set(offsets);
//                      return rdd;
//                    }
//                  }
//                ).flatMap(new FlatMapFunction<String, String>() {
//                    public Iterable<String> call(
//                           String event)
//                            throws Exception {
//                        return Arrays.asList(event);
//                    }
//                });
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
//
//        lines.foreachRDD(new VoidFunction<JavaRDD<String>>(){
//            @Override
//            public void call(JavaRDD<String> t) throws Exception {
//
//                ObjectMapper objectMapper = new ObjectMapper();
//
//                CuratorFramework  curatorFramework = CuratorFrameworkFactory.builder()
//                        .connectString("lxjr-hadoop01:2181,lxjr-hadoop02:2181,lxjr-hadoop03:2181").connectionTimeoutMs(1000)
//                        .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
//
//                curatorFramework.start();
//
//                for (OffsetRange offsetRange : offsetRanges.get()) {
//                    final byte[] offsetBytes = objectMapper.writeValueAsBytes(offsetRange.untilOffset());
//                    String nodePath = "/consumers/spark-group/offsets/" + offsetRange.topic()+ "/" + offsetRange.partition();
//                    if(curatorFramework.checkExists().forPath(nodePath)!=null){
//                            curatorFramework.setData().forPath(nodePath,offsetBytes);
//                        }else{
//                            curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
//                        }
//                }
//
//                curatorFramework.close();
//            }
//
//        });
//
//        wordsCount.print();
//
//        return jsc;
//    }
//
//
//    public static Map<TopicAndPartition,Long> getConsumerOffsets(String zkServers,
//                String groupID, String topic) {
//        Map<TopicAndPartition,Long> retVals = new HashMap<TopicAndPartition,Long>();
//
//        ObjectMapper objectMapper = new ObjectMapper();
//        CuratorFramework  curatorFramework = CuratorFrameworkFactory.builder()
//                .connectString(zkServers).connectionTimeoutMs(1000)
//                .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
//
//        curatorFramework.start();
//
//        try{
//        String nodePath = "/consumers/"+groupID+"/offsets/" + topic;
//        if(curatorFramework.checkExists().forPath(nodePath)!=null){
//            List<String> partitions=curatorFramework.getChildren().forPath(nodePath);
//            for(String partiton:partitions){
//                int partitionL=Integer.valueOf(partiton);
//                Long offset=objectMapper.readValue(curatorFramework.getData().forPath(nodePath+"/"+partiton),Long.class);
//                TopicAndPartition topicAndPartition=new TopicAndPartition(topic,partitionL);
//                retVals.put(topicAndPartition, offset);
//            }
//        }
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//        curatorFramework.close();
//        return retVals;
//    }
//
//    public static Map<TopicAndPartition,Long> getTopicOffsets(String zkServers, String topic){
//        Map<TopicAndPartition,Long> retVals = new HashMap<TopicAndPartition,Long>();
//
//        for(String zkServer:zkServers.split(",")){
//        SimpleConsumer simpleConsumer = new SimpleConsumer(zkServer.split(":")[0],
//                Integer.valueOf(zkServer.split(":")[1]),
//                10000,
//                1024,
//                "consumer");
//        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Arrays.asList(topic));
//        TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);
//
//        for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
//            for (PartitionMetadata part : metadata.partitionsMetadata()) {
//                Broker leader = part.leader();
//                if (leader != null) {
//                    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, part.partitionId());
//
//                    PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 10000);
//                    OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());
//                    OffsetResponse offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest);
//
//                    if (!offsetResponse.hasError()) {
//                        long[] offsets = offsetResponse.offsets(topic, part.partitionId());
//                        retVals.put(topicAndPartition, offsets[0]);
//                    }
//                }
//            }
//        }
//        simpleConsumer.close();
//        }
//        return retVals;
//    }
//
//    public static void main(String[] args)  throws Exception{
//        JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
//            public JavaStreamingContext create() {
//              return createContext();
//            }
//          };
//
//        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate("/c", factory);
//
//        jsc.start();
//
//        jsc.awaitTermination();
//        jsc.close();
//
//    }
//}
