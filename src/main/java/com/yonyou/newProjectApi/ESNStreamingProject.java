package com.yonyou.newProjectApi;

import com.yonyou.utils.SparkUtils;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 优化streaming 设置缓存 优化内部处理结构
 * Created by chenxiaolei on 16/12/26.
 */
public class ESNStreamingProject {
    //设置checkpoint
    private static final String ESNSTREAMING2HBASE = "hdfs://cluster/streaming/syb/ESNStreaming";
    //设置broker list
    private static final String KAFKA_METADATA_BROKER_LIST = "hdslave1:9092,hdslave2:9092,hdmaster:9092";
    private static final String ESNSTREAMING2HBASE_TOPIC = "esn_accesslog";
    private static final String ESN_GROUPID = "sparkStreaming-group-esn";
    private static final String ZOOKEEPER_LIST = "hdslave1:2181,hdslave2:2181,hdmaster:2181";

    //设置批处理次时间
    private static final int ESNSTREAMING2HBASE_TIME = 10;
    private static final long SNSTREAMING2HBASE_OFFSET_NUM = 0;


    //主程序
    public static void main(String[] args) {
        //主代码
        JavaStreamingContextFactory sparkfactory = new JavaStreamingContextFactory() {
            public JavaStreamingContext create() {
                return createContext();
            }
        };
        //ha hdfs获取原先的状态
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(ESNSTREAMING2HBASE, sparkfactory);
        //程序开始
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    /**
     * 过滤 不需要的数据
     *
     * @param line
     * @return 过滤完后的数据
     */
    private static JavaDStream<String> filter(final JavaDStream<String> line) {
        //原始数据按 "\t" 分割 总共20个字段
        //留存 api esn m等等5种数据 有需求 在增加 除去 .htm .js .css .png .image 等
        return line.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v) throws Exception {
                String[] lines = v.split("\t");
                //判断长度 不符合 干掉
                if (lines.length==20) {
                    if ("esn".equals(lines[3])){
                        return true;
                    }else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });
    }

    /**
     * 运行主要程序代码
     *
     * @return JavaStreamingContext实例
     */
    public static JavaStreamingContext createContext() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("BrowserUPVEveryDay")
                .set("spark.default.parallelism", "150")//並行度，reparation后生效(因为集群现在的配置是8核，按照每个核心有一个vcore，就是16，三个worker节点，就是16*3，并行度设置为3倍的话：16*3*3=144，故，这里设置150)
                .set("spark.locality.wait", "100ms")
                .set("spark.shuffle.manager", "hash")//使用hash的shufflemanager
                .set("spark.shuffle.consolidateFiles", "true")//shufflemap端开启合并较小落地文件（hashshufflemanager方式一个task对应一个文件，开启合并，reduce端有几个就是固定几个文件，提前分配好省着merge了）
                .set("spark.shuffle.file.buffer", "64")//shufflemap端mini环形缓冲区bucket的大小调大一倍，默认32KB
                .set("spark.reducer.maxSizeInFlight", "24")//从shufflemap端拉取数据24，默认48M
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//序列化
                .set("spark.shuffle.io.maxRetries", "10")//GC重试次数，默认3
                .set("spark.shuffle.io.retryWait", "30s")//GC等待时长，默认5s
                ;
        //设置批次时间 10s
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(ESNSTREAMING2HBASE_TIME));
        //设置spark容错点
        jssc.checkpoint(ESNSTREAMING2HBASE);
        //构建kafka参数 broker list
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", KAFKA_METADATA_BROKER_LIST);
        //设置参数 设置或获取topic表数据
        final Map<TopicAndPartition, Long> topicOffsets = SparkUtils.getTopicOffsets(KAFKA_METADATA_BROKER_LIST, ESNSTREAMING2HBASE_TOPIC);

        Map<TopicAndPartition, Long> consumerOffsets = SparkUtils.getConsumerOffsets(ZOOKEEPER_LIST, ESN_GROUPID, ESNSTREAMING2HBASE_TOPIC);
        if (null != consumerOffsets && consumerOffsets.size() > 0) {
            topicOffsets.putAll(consumerOffsets);
        }

        for (Map.Entry<TopicAndPartition, Long> item : topicOffsets.entrySet()) {
            item.setValue(item.getValue() - SNSTREAMING2HBASE_OFFSET_NUM);
        }
        //打印topicoffset信息
        for (Map.Entry<TopicAndPartition, Long> entry : topicOffsets.entrySet()) {
            System.out.println(entry.getKey().topic() + "\t" + entry.getKey().partition() + "\t" + entry.getValue());
        }
        //获取到数据
        JavaInputDStream<String> lines = KafkaUtils.createDirectStream(jssc,
                String.class, String.class, StringDecoder.class,
                StringDecoder.class, String.class, kafkaParams,
                topicOffsets, new Function<MessageAndMetadata<String, String>, String>() {

                    public String call(MessageAndMetadata<String, String> v1)
                            throws Exception {
                        return v1.message();
                    }
                });
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        JavaDStream<String> line = lines.transform(
                new Function<JavaRDD<String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);
                        return rdd;
                    }
                }
        );


        // 1 过滤数据源
        JavaDStream<String> filter = filter(line);
        return null;
    }


}
