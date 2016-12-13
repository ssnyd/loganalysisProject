package com.yonyou.newProjectApi;

import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import com.yonyou.conf.ConfigurationManager;
import com.yonyou.constant.Constants;
import com.yonyou.dao.ILogStatDAO;
import com.yonyou.dao.factory.DAOFactory;
import com.yonyou.hbaseUtil.HbaseConnectionFactory;
import com.yonyou.jdbc.JDBCUtils;
import com.yonyou.utils.*;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hive.ql.parse.HiveParser.ifExists_return;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by ChenXiaoLei on 2016/11/23.
 */
public class ESNStreaming2HbaseOptimize {

    //add by song 2016.12.05
    /*
	 * 解决新老版本的兼容性问题
	 */
    //private static final long serialVersionUID=7981560250804078637l;
    //end by song 2016.12.05

    public static void main(String[] args) {
        JavaStreamingContextFactory sparkfactory = new JavaStreamingContextFactory() {
            public JavaStreamingContext create() {
                return createContext();
            }
        };
        // Get StreaminContext from checkpoint data or create a new one(driver's HA)
        //change by song 2016.12.08
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE), sparkfactory);
        //JavaStreamingContext jssc = JavaStreamingContext.getOrCreate("hdfs://cluster/streamingTest/esnStreamingTest", sparkfactory);
        //end by song 2016.12.08
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    public static JavaStreamingContext createContext() {
        SparkConf conf = new SparkConf()
                .setAppName("ESNStreaming2HbaseOptimize")
                /*
                 * change by song 2016.12.05
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//使用kryo压缩
                .set("spark.streaming.blockInterval", "200")//ms→RDD；最小值是50ms，减少block interval可以提高task数，即提高了并发度
                .set("spark.streaming.unpersist", "true")//默认是true，更智能地去持久化（unpersist）RDD
                .set("spark.shuffle.io.maxRetries", "60")//JVM的GC的导致的shuffle文件拉取失败后的重试次数，默认3次
                .set("spark.shuffle.io.retryWait", "60s")//JVM的GC的导致的shuffle文件拉取失败后隔多少秒重试一次，默认5秒
                .set("spark.reducer.maxSizeInFlight", "24");//shuffle的reduce端每次抓取的数据量的缓冲大小，默认48MB指定为24MB，我宁愿多拉取几次，但是每次同时能够拉取到reduce端每个task的数量，比较少
                */
                /*
                 * add by song 2016.12.05
                 */
                //add by song 2016.12.05 调优
                .set("spark.default.parallelism", "150")//並行度，reparation后生效(因为集群现在的配置是8核，按照每个核心有一个vcore，就是16，三个worker节点，就是16*3，并行度设置为3倍的话：16*3*3=144，故，这里设置150)
                .set("spark.streaming.unpersist", "true")//默认是true，更智能地去持久化（unpersist）RDD,PS:清除已经持久化的RDD数据,1.4之前用spark.cleaner.ttl，之后被废弃，现在用spark.streaming.unpersist
//                .set("spark.memory.useLegacyMode", "true")//内存模型使用spark1.5版本的老版本的内存模型，这样memoryFraction/memoryFraction才能生效！！！
//                .set("spark.storage.memoryFraction", "0.5")//留给cache做缓存或持久化用的内存的比例是多少，默认是0.6
//                .set("spark.shuffle.memoryFraction", "0.3")//留给shuffle的内存是多少，默认是0.2，因为shuffle有可能排序或者一些操作，多给点内存比例会快一点
                .set("spark.locality.wait", "100ms")
                .set("spark.shuffle.manager", "hash")//使用hash的shufflemanager
                .set("spark.shuffle.consolidateFiles", "true")//shufflemap端开启合并较小落地文件（hashshufflemanager方式一个task对应一个文件，开启合并，reduce端有几个就是固定几个文件，提前分配好省着merge了）
                .set("spark.shuffle.file.buffer", "128")//shufflemap端mini环形缓冲区bucket的大小调大一倍，默认32KB
                .set("spark.reducer.maxSizeInFlight", "96")//从shufflemap端拉取数据24，默认48M
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//序列化
                .set("spark.shuffle.io.maxRetries", "10")//GC重试次数，默认3
                .set("spark.shuffle.io.retryWait", "30s")//GC等待时长，默认5s
                .set("spark.streaming.stopGracefullyOnShutdown", "true")//优雅
                .set("spark.streaming.blockInterval", "100")//一個blockInterval对应一个task(direct方式没用)
                .set("spark.streaming.kafka.maxRatePerPartition", "10000")//限制消息消费的速率
                .set("spark.streaming.backpressure.enabled", "true");//Spark Streaming从v1.5开始引入反压机制（back-pressure）,通过动态控制数据接收速率来适配集群数据处理能力。
//				.set("spark.speculation","true");//解决纠结的任务，跟mapreduce处理方案一样啊
        //conf.setMaster("local[2]");//本地测试

        //batch_interval根据my.properties中的参数设置：ESNStreaming2Hbase.time=5
        //add by song 2016.12.05
        //direct方式并行度与kafka的partition分区数一致，spark.default.parallelism设置的并行度在repartition后生效，repartition有shuffle啊，但是没办法啊！！！
        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(Integer.parseInt(ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE_TIME))));
        //设置spark容错点
        jssc.checkpoint(ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
//        String[] kafkaTopicsSplited = kafkaTopics.split(",");
//        Set<String> topics = new HashSet<String>();

        final Map<TopicAndPartition, Long> topicOffsets = SparkUtils.getTopicOffsets(ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST), ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE_TOPIC));

        Map<TopicAndPartition, Long> consumerOffsets = SparkUtils.getConsumerOffsets(ConfigurationManager.getProperty(Constants.ZOOKEEPER_LIST), ConfigurationManager.getProperty(Constants.ESN_GROUPID), ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE_TOPIC));
        if (null != consumerOffsets && consumerOffsets.size() > 0) {
            topicOffsets.putAll(consumerOffsets);
        }

        for (Map.Entry<TopicAndPartition, Long> item : topicOffsets.entrySet()) {
            item.setValue(item.getValue() - Long.parseLong(ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE_OFFSET_NUM)));
        }
        //打印topicoffset信息
        for (Map.Entry<TopicAndPartition, Long> entry : topicOffsets.entrySet()) {
            System.out.println(entry.getKey().topic() + "\t" + entry.getKey().partition() + "\t" + entry.getValue());
        }
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
        line.repartition(150);
        //end by song 2016.12.05

        //去除脏数据，保留合格的一帧数据
        JavaDStream<String> filter = filterByRequest(line);
        JavaPairDStream<String, String> modify = modifyByIPAndToken(filter);
        //change by song 2016.12.09
//        modify = modify.persist(StorageLevel.MEMORY_AND_DISK_SER());
        modify = modify.persist(StorageLevel.MEMORY_ONLY());
        //end by song 2016.12.05
        JavaPairDStream<String, String> PVStat = calculatePVSta(modify);

        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {

            private static final long serialVersionUID = 1L;

            public void call(JavaRDD<String> t) throws Exception {

                //add by song 2015.12.05
				/*
				 * 调优
				 */
                if (!t.isEmpty()) {
                    //add by song 2016.12.09
                    try {
                        //end by song 2016.12.09
                        ObjectMapper objectMapper = new ObjectMapper();
                        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                                .connectString(ConfigurationManager.getProperty(Constants.ZOOKEEPER_LIST)).connectionTimeoutMs(1000)
                                .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
                        curatorFramework.start();
                        for (OffsetRange offsetRange : offsetRanges.get()) {
                            final byte[] offsetBytes = objectMapper.writeValueAsBytes(offsetRange.untilOffset());
                            //change by song 2016.12.09
                            String nodePath = "/consumers/" + ConfigurationManager.getProperty(Constants.ESN_GROUPID) + "/offsets/" + offsetRange.topic() + "/" + offsetRange.partition();
                            // String nodePath = "/consumers/"+ "esnStreaming2Hbase_test" +"/offsets/" + offsetRange.topic()+ "/" + offsetRange.partition();
                            //end by song 2016.12.09
                            if (curatorFramework.checkExists().forPath(nodePath) != null) {
                                curatorFramework.setData().forPath(nodePath, offsetBytes);
                            } else {
                                curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
                            }
                        }

                        curatorFramework.close();
                        //add by song 2016.12.09
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    //end by song 2016.12.09
                }

                //end by song 2015.12.05
            }
        });
        return jssc;
    }

    /**
     * PV to mysql
     *
     * @param modifyLogDStream
     * @return
     */
    @SuppressWarnings("deprecation")
    private static JavaPairDStream<String, String> calculatePVSta(JavaPairDStream<String, String> modifyLogDStream) {
        //value eg:原数据+"country:"+country+"\tregion:"+region+"\tcity:"+city+"member_id:"+member_id+"\tqz_id:"+qz_id+"\tuser_id:"+user_id+"\tinstance_id:"+instance_id
        //key eg:2016:11:29:13:52:50:172.20.1.177:esn:1480398770.021
        JavaPairDStream<String, String> filterDStream = modifyLogDStream.filter(new Function<Tuple2<String, String>, Boolean>() {

            private static final long serialVersionUID = 1L;

            public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                if (tuple2 != null) {
                    return true;
                }
                return false;
            }
        });
        JavaPairDStream<String, Integer> mapPairDStream = filterDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                String log = tuple2._2;
                String[] logSplited = log.split("\t");
                long timestamp = getTime(logSplited[7], 0);
                String region = getCity(logSplited[21].split(":")[1]);
                String key = timestamp + "%" + region;
                return new Tuple2<String, Integer>(key, 1);//key:时间戳+城市，一个batch中时间戳出现大量重复，城市出现大量重复

            }
        })//change by song 2016.12.07
                .reduceByKey(new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    public Integer call(Integer v1, Integer v2) throws Exception {
                        // TODO Auto-generated method stub
                        return v1 + v2;
                    }
//		}, 150);
                }, 150);
        /*
         *change by song 2016.12.07
        .reduceByKey(new Function2<Integer, Integer, Integer>() {//shuffle,map端有combiner
        	
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });*/
        //change by song 2016.12.07 改善data skew
        //reduceByKey(func(),150);
        //end by song 2016.12.07
        JavaPairDStream<String, String> maptopair = mapPairDStream.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, String> call(Tuple2<String, Integer> tuple) throws Exception {
                String[] split = tuple._1.split("%");
                String timestamp = split[0];
                String region = split[1];
                return new Tuple2<String, String>(timestamp, region + "&" + tuple._2);
            }
        })//change by song 2016.12.07
                .reduceByKey(new Function2<String, String, String>() {

                    private static final long serialVersionUID = 1L;

                    public String call(String s, String s2) throws Exception {
                        // TODO Auto-generated method stub
                        return s + "#" + s2;
                    }
                }, 150);
        // });
        /*
         * change by song 2016.12.07
        .reduceByKey(new Function2<String, String, String>() {//shuffle,map端有combiner
        	
			private static final long serialVersionUID = 1L;

			public String call(String s, String s2) throws Exception {
                return s + "#" + s2;
            }
        });*/
        maptopair.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
            private static final long serialVersionUID = 1L;

            public Void call(JavaPairRDD<String, String> rdd) throws Exception {

                //add by song 2015.12.05
            	/*
            	 * 调优
            	 */
                if (!rdd.isEmpty()) {

                    rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {

                        private static final long serialVersionUID = 1L;

                        public void call(Iterator<Tuple2<String, String>> tuples) throws Exception {
                            List<Map<String, String>> pvStats = new ArrayList<Map<String, String>>();
                            Tuple2<String, String> tuple = null;
                            while (tuples.hasNext()) {
                                Map<String, String> pvStat = new HashMap<String, String>();
                                tuple = tuples.next();
                                String[] region_count = tuple._2.split("#");
                                pvStat.put("timestamp", tuple._1);
                                for (String str : region_count) {
                                    String[] s = str.split("&");
                                    if (pvStat.get(s[0]) != null) {
                                        int num = Integer.parseInt(pvStat.get(s[0])) + Integer.parseInt(s[1]);
                                        pvStat.put(s[0], num + "");
                                    } else {
                                        pvStat.put(s[0], s[1]);
                                    }
                                }
                                pvStats.add(pvStat);
                            }
                            if (pvStats.size() > 0) {
                                JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                                Connection conn = jdbcUtils.getConnection();
                                ILogStatDAO logStatDAO = DAOFactory.getLogStatDAO();
                                logStatDAO.updataBatch(pvStats, conn);
                                System.out.println("mysql  to pvstat ==> " + pvStats.size());
                                pvStats.clear();
                                if (conn != null) {
                                    jdbcUtils.closeConnection(conn);
                                }
                            }

                            //add by song 2016.12.09	优化
                            pvStats = null;
                            //end by song 2016.12.09
                        }
                    });

                    //end by song 2015.12.05
                }
                return null;
            }
        });
        return maptopair;
    }

    private static long getTime(String timestamp, int num) {
        String strDateTime = timestamp.replace("[", "").replace("]", "");
        long datekey = 0l;
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat day = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd HH");
        Date t = null;
        String format = "";
        try {
            t = formatter.parse(strDateTime);
            if (1 == num) {
                format = day.format(t);
                t = day.parse(format);
            } else if (2 == num) {
                format = hour.format(t);
                t = hour.parse(format);
            }
            datekey = t.getTime() / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return datekey;
    }


    /**
     * filterData
     *
     * @param line
     * @return
     */
    /*
     * 从kafka中每隔5s为一个batch_interval封装一个RDD，将RDD中的每行数据，执行一个filter算子将满足条件的一帧数据保留，
     * 取出脏数据。
     */
    private static JavaDStream<String> filterByRequest(JavaDStream<String> line) {
        return line.filter(new Function<String, Boolean>() {

            private static final long serialVersionUID = 1L;

            public Boolean call(String v1) throws Exception {
                String[] log = v1.split("\t");
                boolean flag = false;
                if (log.length == 20 && "esn".equals(log[3])) {
                    flag = log[19].contains("PHPSESSID");
                } else if (log.length == 20 && ("api".equals(log[3]) || "h5-api".equals(log[3]) || "pc-api".equals(log[3]) || "m".equals(log[3]))) {
                    flag = v1.contains("token");
                }
                return (log[9].contains(".htm") || ((!(v1.contains(".js ") || v1.contains(".css") || v1.contains(".js?") || v1.contains(".png") || v1.contains(".gif"))) && flag && 1 == 1));
            }
        });
    }

    /**
     * 增加新的字段 地址加3ID \t 拼接原始数据
     *
     * @param filter
     * @return
     */
    private static JavaPairDStream<String, String> modifyByIPAndToken(JavaDStream<String> filter) {
        JavaPairDStream<String, String> modify = filter.mapToPair(new PairFunction<String, String, String>() {

            private static final long serialVersionUID = 1L;

            public Tuple2<String, String> call(String s) throws Exception {
                String token = "";
                String[] lines = s.split("\t");
                String mquID = "member_id:empty\tqz_id:empty\tuser_id:empty";
                String remote_addr = "country:empty\tregion:empty\tcity:empty\tinstance_id:empty";
                //求ip
                String ip = lines[0];
                if (!"".equals(ip) && ip.length() < 16) {//判断IP不脏
                    String result = "";
                    try {
                        result = HttpReqUtil.getResult("ip/query", ip);
                        try {
                            //调用PHP接口，PHP返回json串，解析该json串，得到eg：
                            //country:"+country+"\tregion:"+region+"\tcity:"+city的字符串
                            remote_addr = JSONUtil.getIPStr(result);
                        } catch (Exception e) {
                            System.out.println(result + " ==> 解析 ip → json 错误");
                            System.out.println(ip + " ==> json→ip");
                        }
                    } catch (Exception e) {
                        System.out.println(ip + " ==> read time out! 数据跳过");
                    }
                }
                //求mqu
                if (lines[9].contains(".htm")) {//RDD中的一行中包含".htm"，便不予处理
                    System.out.println("存在htm 不分析");
                } else if ("esn".equals(lines[3])) {
                    //lines[19]eg："PHPSESSID=67ivh3pr0oq32hh17p5klokrk5; path=/"
                    if (lines[19].split(" ")[0].contains("PHPSESSID")) {
                        token = lines[19].split(" ")[0].split("=")[1].split(";")[0];
                    }
                    if (!"".equals(token) && !token.contains("/") && !token.contains("\\")) {
                        String result = "";
                        try {
                            result = HttpReqUtil.getResult("user/redis/esn/" + token, "");
                            //调用PHP接口，PHP返回json串，解析该json串，得到eg：
                            //"member_id:"+member_id+"\tqz_id:"+qz_id+"\tuser_id:"+user_id+"\tinstance_id:"+instance_id
                            mquID = JSONUtil.getmquStr(result);
                        } catch (Exception e) {
                            System.out.println(token + "read time out ! token数据跳过");
                            System.out.println(result + " ==> json→token");
                        }
                    }
                } else {
                    if ("api".equals(lines[3])) {
                        token = ESNStreaming2HbaseOptimize.Token(lines);
                        if (token.equals(""))
                            try {
                                //lines[9]eg:
                                //"POST /rest/app/getList3 HTTP/1.1"
                                //"GET /rest/user/getMyActiveInfo?access_token=f7dccdf70b53297a5ce9dc63e637a4c56c289e0a&appType=1&ek=b8bdffa4b6690fe7ccdc5bb12a64be36&et=1480398779&member_id=198659&qz_id=4243&vercode=1-3.0.6-1-1 HTTP/1.1"
                                //判断GET请求
                                if ((lines[9].split(" ").length >= 3) && (lines[9].contains("?")) && (lines[9].contains("token"))) {
                                    URL aURL = new URL("http://test.com" + lines[9].split(" ")[1]);
                                    //拼接后的aURL:
                                    //eg:http://test.com/rest/user/getMyActiveInfo?access_token=f7dccdf70b53297a5ce9dc63e637a4c56c289e0a&appType=1&ek=b8bdffa4b6690fe7ccdc5bb12a64be36&et=1480398779&member_id=198659&qz_id=4243&vercode=1-3.0.6-1-1 HTTP/1.1
                                    if ((!"".equals(aURL.getQuery())) && (aURL.getQuery().contains("token"))) {
                                        String[] str = aURL.getQuery().split("&");
                                        for (int i = 0; i < str.length; i++)
                                            if ("access_token".equals(str[i].split("=")[0]) && str[i].split("=").length == 2) {
                                                token = str[i].split("=")[1];
                                                break;
                                            }
                                    }
                                }
                            } catch (MalformedURLException e) {
                                e.printStackTrace();
                            }
                    } else {
                        token = ESNStreaming2HbaseOptimize.Token(lines);
                        if ("".equals(token)) {
                            try {
                                if ((lines[9].split(" ").length >= 3) && (lines[9].contains("?")) && (lines[9].contains("token"))) {
                                    URL aURL = new URL("http://test.com" + lines[9].split(" ")[1]);
                                    if ((!"".equals(aURL.getQuery())) && (aURL.getQuery().contains("token"))) {
                                        String[] str = aURL.getQuery().split("&");
                                        for (int i = 0; i < str.length; i++)
                                            if ("token".equals(str[i].split("=")[0]) && str[i].split("=").length == 2) {
                                                token = str[i].split("=")[1];
                                                break;
                                            }
                                    }
                                }
                            } catch (MalformedURLException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    if (!"".equals(token) && !token.contains("/") && !token.contains("\\")) {
                        //调用PHP接口，PHP返回json串，解析该json串，得到eg：
                        //"member_id:"+member_id+"\tqz_id:"+qz_id+"\tuser_id:"+user_id+"\tinstance_id:"+instance_id
                        mquID = JSONUtil.getmquStr(HttpReqUtil.getResult("user/redis/api/" + token, ""));
                    }
                }
                if (!remote_addr.contains("局域网")) {
                    String value = s + "\t" + remote_addr + "\t" + mquID;
                    //eg:lines[7]
                    //[29/Nov/2016:13:52:50 +0800]
                    //getTime后格式：yyyy:MM:dd:HH:mm:ss
                    String times = getTime(lines[7]);
                    System.out.println(times+":"+lines[2] + ":" + lines[3] + ":" + lines[8] + "&&" + token + "==>" + lines[9]);
                    //lines[2] eg:172.20.1.177
                    //lines[3] eg:esn
                    //lines[8] eg:1480398770.021
                    //value eg:原数据+"country:"+country+"\tregion:"+region+"\tcity:"+city+"member_id:"+member_id+"\tqz_id:"+qz_id+"\tuser_id:"+user_id+"\tinstance_id:"+instance_id
                    //key eg:2016:11:29:13:52:50:172.20.1.177:esn:1480398770.021
                    return new Tuple2<String, String>(times + ":" + lines[2] + ":" + lines[3] + ":" + lines[8], value);
                } else {
                    return null;
                }
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {

            private static final long serialVersionUID = 1L;

            public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                if (tuple2 != null) {
                    return true;
                }
                return false;
            }
        });
        modify.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

            private static final long serialVersionUID = 1L;

            public void call(JavaPairRDD<String, String> rdd) throws Exception {

                //add by song 2015.12.05
            	/*
            	 * 调优
            	 */
                if (!rdd.isEmpty()) {

                    rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {

                        private static final long serialVersionUID = 1L;

                        public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                            Tuple2<String, String> tuple = null;
                            List<Put> puts = new ArrayList<Put>();
                            while (tuple2Iterator.hasNext()) {
                                tuple = tuple2Iterator.next();
                                if (tuple != null) {
                                    //rowkey:tuple._1    eg:2016:11:29:13:52:50:172.20.1.177:esn:1480398770.021
                                    Put put = new Put((String.valueOf(tuple._1)).getBytes());
                                    //列族：accesslog；列：info；值：tuple._2    eg:原数据+"country:"+country+"\tregion:"+region+"\tcity:"+city+"member_id:"+member_id+"\tqz_id:"+qz_id+"\tuser_id:"+user_id+"\tinstance_id:"+instance_id
                                    put.addColumn(Bytes.toBytes("accesslog"), Bytes.toBytes("info"), Bytes.toBytes(tuple._2));
                                    puts.add(put);
                                }
                            }
                            if (puts.size() > 0) {
                                HTable hTable = HbaseConnectionFactory.gethTable("esn_accesslog", "accesslog");
                                //change by song 2016.12.05
                                hTable.put(puts);
                                hTable.flushCommits();
                                //end by song 2016.12.05
                                System.out.println("hbase ==> " + puts.size());
                                puts.clear();
                                if (hTable != null) {
                                    hTable.close();
                                }
                            }

                            //add by song 2016.12.09	优化
                            puts = null;
                            //end by song 2016.12.09

                        }
                    });

                    //end by song 2015.12.05
                }
            }

        });
        return modify;
    }


    /**
     * 获取请求中的token
     *
     * @param lines
     * @return
     */
    public static String Token(String[] lines) {
        String _token = "";
        //lines[10]eg：
        //{\x22ek\x22:\x22fe657e562ce31f17dc2bb289afd1c81c\x22,\x22access_token\x22:\x2250a4360790acd695e6d1de9850d4dbfbd337bd1a\x22,\x22qz_id\x22:\x221\x22,\x22et\x22:\x221480398797\x22,\x22appType\x22:\x221\x22,\x22vercode\x22:\x221-3.0.6-1-1\x22}
        String s1 = lines[10].replace("\\x22", "\"").replace("\"", "").replace("\\x5C", "");
        //转换后：
        //{"ek":"fe657e562ce31f17dc2bb289afd1c81c","access_token":"50a4360790acd695e6d1de9850d4dbfbd337bd1a","qz_id":"1","et":"1480398797","appType":"1","vercode":"1-3.0.6-1-1"}
        String[] split = s1.split(",");
        for (int i = 0; i < split.length; i++) {
            if (split[i].contains("token")) {
                if (split[i].split(":").length == 2)
                    _token = split[i].split(":")[1].replace("}", "").replace("{", "");
                break;
            }
        }
        return _token;
    }

    public static String getCity(String city) {
        String str = "haiwai";
        if ("澳门".equals(city)) {
            str = "aomen";
        } else if ("香港".equals(city)) {
            str = "xianggang";
        } else if ("台湾".equals(city)) {
            str = "taiwan";
        } else if ("广东".equals(city)) {
            str = "guangdong";
        } else if ("广西".equals(city)) {
            str = "guangxi";
        } else if ("海南".equals(city)) {
            str = "hainan";
        } else if ("云南".equals(city)) {
            str = "yunnan";
        } else if ("福建".equals(city)) {
            str = "fujian";
        } else if ("江西".equals(city)) {
            str = "jiangxi";
        } else if ("湖南".equals(city)) {
            str = "hunan";
        } else if ("贵州".equals(city)) {
            str = "guizhou";
        } else if ("浙江".equals(city)) {
            str = "zhejiang";
        } else if ("安徽".equals(city)) {
            str = "anhui";
        } else if ("上海".equals(city)) {
            str = "shanghai";
        } else if ("江苏".equals(city)) {
            str = "jiangsu";
        } else if ("湖北".equals(city)) {
            str = "hubei";
        } else if ("西藏".equals(city)) {
            str = "xizang";
        } else if ("青海".equals(city)) {
            str = "qinghai";
        } else if ("陕西".equals(city)) {
            str = "shanxi_shan";
        } else if ("山西".equals(city)) {
            str = "shanxi_jin";
        } else if ("河南".equals(city)) {
            str = "henan";
        } else if ("山东".equals(city)) {
            str = "shandong";
        } else if ("河北".equals(city)) {
            str = "hebei";
        } else if ("天津".equals(city)) {
            str = "tianjin";
        } else if ("北京".equals(city)) {
            str = "beijing";
        } else if ("宁夏".equals(city)) {
            str = "ningxia";
        } else if ("内蒙古".equals(city)) {
            str = "neimeng";
        } else if ("辽宁".equals(city)) {
            str = "liaoning";
        } else if ("吉林".equals(city)) {
            str = "jilin";
        } else if ("黑龙江".equals(city)) {
            str = "heilongjiang";
        } else if ("重庆".equals(city)) {
            str = "chongqing";
        } else if ("西川".equals(city)) {
            str = "sichuan";
        } else if ("局域网".equals(city)) {
            str = "local";
        }
        return str;
    }

    public static String getTime(String timestamp) {
        //timestamp eg:[29/Nov/2016:13:52:50 +0800]
        String strDateTime = timestamp.replace("[", "").replace("]", "");
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat hour = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss");
        Date parse = null;
        try {
            parse = formatter.parse(strDateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return hour.format(parse);
    }

    private static long getDayLong(long time) {
        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd");
        long day = 0l;
        try {
            String format = hour.format(time * 1000);
            Date date = new Date(time * 1000);

            Date parse = hour.parse(format);
            day = parse.getTime() / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return day;
    }


}
