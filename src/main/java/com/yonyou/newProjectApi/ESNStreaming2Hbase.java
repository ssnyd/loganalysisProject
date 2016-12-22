package com.yonyou.newProjectApi;

import com.yonyou.conf.ConfigurationManager;
import com.yonyou.constant.Constants;
import com.yonyou.dao.ILogStatDAO;
import com.yonyou.dao.factory.DAOFactory;
import com.yonyou.hbaseUtil.HbaseConnectionFactory;
import com.yonyou.jdbc.JDBCUtils;
import com.yonyou.utils.HttpReqUtil;
import com.yonyou.utils.IdCrypt;
import com.yonyou.utils.JSONUtil;
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
public class ESNStreaming2Hbase {
    public static void main(String[] args) {
        JavaStreamingContextFactory sparkfactory = new JavaStreamingContextFactory() {
            public JavaStreamingContext create() {
                return createContext();
            }
        };
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(ConfigurationManager.getProperty(Constants.ESNSTREAMING2HBASE), sparkfactory);
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    /*
     * add by song 2016.12.05
     */
    public static JavaStreamingContext createContext() {
        SparkConf conf = new SparkConf()
                .setAppName("ESNStreaming2Hbase")
                //add by song 2016.12.05 调优
                //.set("spark.default.parallelism", "150")//並行度，reparation后生效(因为集群现在的配置是8核，按照每个核心有一个vcore，就是16，三个worker节点，就是16*3，并行度设置为3倍的话：16*3*3=144，故，这里设置150)
                //.set("spark.streaming.unpersist", "true")//默认是true，更智能地去持久化（unpersist）RDD,PS:清除已经持久化的RDD数据,1.4之前用spark.cleaner.ttl，之后被废弃，现在用spark.streaming.unpersist
//                .set("spark.memory.useLegacyMode", "true")//内存模型使用spark1.5版本的老版本的内存模型，这样memoryFraction/memoryFraction才能生效！！！
//                .set("spark.storage.memoryFraction", "0.5")//留给cache做缓存或持久化用的内存的比例是多少，默认是0.6
                .set("spark.shuffle.memoryFraction", "0.3")//留给shuffle的内存是多少，默认是0.2，因为shuffle有可能排序或者一些操作，多给点内存比例会快一点
                .set("spark.locality.wait", "100ms")
                .set("spark.shuffle.manager", "hash")//使用hash的shufflemanager
//                .set("spark.shuffle.consolidateFiles", "true")//shufflemap端开启合并较小落地文件（hashshufflemanager方式一个task对应一个文件，开启合并，reduce端有几个就是固定几个文件，提前分配好省着merge了）
//                .set("spark.shuffle.file.buffer", "128")//shufflemap端mini环形缓冲区bucket的大小调大一倍，默认32KB
//                .set("spark.reducer.maxSizeInFlight", "96")//从shufflemap端拉取数据24，默认48M
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//序列化
//                .set("spark.shuffle.io.maxRetries", "10")//GC重试次数，默认3
//                .set("spark.shuffle.io.retryWait", "30s")//GC等待时长，默认5s
//                .set("spark.streaming.stopGracefullyOnShutdown", "true")//优雅
//                .set("spark.streaming.blockInterval", "100")//一個blockInterval对应一个task(direct方式没用)
                .set("spark.streaming.kafka.maxRatePerPartition", "10000")//限制消息消费的速率
                .set("spark.streaming.backpressure.enabled", "true")//Spark Streaming从v1.5开始引入反压机制（back-pressure）,通过动态控制数据接收速率来适配集群数据处理能力。
//				.set("spark.speculation","true");//解决纠结的任务，跟mapreduce处理方案一样啊
                //conf.setMaster("local[2]");//本地测试

                //batch_interval根据my.properties中的参数设置：ESNStreaming2Hbase.time=5
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.streaming.blockInterval", "200")//ms→RDD
                .set("spark.streaming.unpersist", "true")
                .set("spark.shuffle.io.maxRetries", "60")
                .set("spark.shuffle.io.retryWait", "60s")
                .set("spark.reducer.maxSizeInFlight", "24");
//      conf.setMaster("local[2]");//本地测试
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
        //过滤数据源
        JavaDStream<String> filter = filterByRequest(line);
        //修改数据源 新增地区和4id
        JavaPairDStream<String, String> modify = modifyByIPAndToken(filter);
        //缓存数据源
        modify = modify.persist(StorageLevel.MEMORY_AND_DISK_SER());
        //计算pv
        JavaPairDStream<String, String> PVStat = calculatePVSta(modify);
        //存储zookeeper offset
        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> t) throws Exception {
                ObjectMapper objectMapper = new ObjectMapper();
                CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                        .connectString(ConfigurationManager.getProperty(Constants.ZOOKEEPER_LIST)).connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
                curatorFramework.start();
                for (OffsetRange offsetRange : offsetRanges.get()) {
                    final byte[] offsetBytes = objectMapper.writeValueAsBytes(offsetRange.untilOffset());
                    String nodePath = "/consumers/" + ConfigurationManager.getProperty(Constants.ESN_GROUPID) + "/offsets/" + offsetRange.topic() + "/" + offsetRange.partition();
                    if (curatorFramework.checkExists().forPath(nodePath) != null) {
                        curatorFramework.setData().forPath(nodePath, offsetBytes);
                    } else {
                        curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
                    }
                }
                curatorFramework.close();
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
    private static JavaPairDStream<String, String> calculatePVSta(JavaPairDStream<String, String> modifyLogDStream) {
        JavaPairDStream<String, String> filterDStream = modifyLogDStream.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                if (tuple2 != null) {
                    return true;
                }
                return false;
            }
        });
        JavaPairDStream<String, Integer> mapPairDStream = filterDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                String log = tuple2._2;
                String[] logSplited = log.split("\t");
                long timestamp = getTime(logSplited[7], 0);
                String region = getCity(logSplited[21].split(":")[1]);
                String key = timestamp + "%" + region;
                return new Tuple2<String, Integer>(key, 1);

            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairDStream<String, String> maptopair = mapPairDStream.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Integer> tuple) throws Exception {
                String[] split = tuple._1.split("%");
                String timestamp = split[0];
                String region = split[1];
                return new Tuple2<String, String>(timestamp, region + "&" + tuple._2);
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + "#" + s2;
            }
        });
        maptopair.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Void call(JavaPairRDD<String, String> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                    @Override
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

                    }
                });
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
    private static JavaDStream<String> filterByRequest(JavaDStream<String> line) {
        return line.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] log = v1.split("\t");
                boolean flag = false;
                if (log.length == 20 && "esn".equals(log[3])) {
                    flag = log[19].contains("PHPSESSID");
                } else if (log.length == 20 && ("api".equals(log[3]) || "h5-api".equals(log[3]) || "pc-api".equals(log[3]) || "m".equals(log[3]))) {
                    flag = v1.contains("token");
                } else if (log.length == 20 && "openapi".equals(log[3])) {
                    flag = true;
                }
                return (v1.contains(".htm") || ((!(v1.contains(".js ") || v1.contains(".css") || v1.contains(".js?") || v1.contains(".png") || v1.contains(".gif"))) && flag && 1 == 1));
            }
        });
    }

    /**
     * 增加新的字段 地区字段加4ID \t 拼接原始数据
     *
     * @param filter
     * @return
     */
    private static JavaPairDStream<String, String> modifyByIPAndToken(JavaDStream<String> filter) {
        JavaPairDStream<String, String> modify = filter.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String token = "";
                String[] lines = s.split("\t");
                String mquID = "member_id:empty\tqz_id:empty\tuser_id:empty\tinstance_id:empty";
                String remote_addr = "country:empty\tregion:empty\tcity:empty";
                //求ip
                String ip = lines[0];
                if (!"".equals(ip) && ip.length() < 16) {
                    String result = "";
                    try {
                        result = HttpReqUtil.getResult("ip/query", ip);
                        try {
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
                //遇见.htm请求的不分析直接存空
                if (lines[9].contains(".htm")) {
                    System.out.println("存在htm 不分析");
                }
                //计算openapi 最后 20位 字段qz_id=80298882&instance_id=78136078&member_id=68175624 需要反编译
                else if ("openapi".equals(lines[3])) {
                    mquID = getOpenApi(lines[19]);
                }
                //针对esn处理 找到PHPSESSID 调用user/redis/esn/"
                else if ("esn".equals(lines[3])) {
                    if (lines[19].split(" ")[0].contains("PHPSESSID")) {
                        token = lines[19].split(" ")[0].split("=")[1].split(";")[0];
                    }
                    if (!"".equals(token) && !token.contains("/") && !token.contains("\\")) {
                        String result = "";
                        try {
                            result = HttpReqUtil.getResult("user/redis/esn/" + token, "");
                            mquID = JSONUtil.getmquStr(result);
                        } catch (Exception e) {
                            System.out.println(token + "read time out ! token数据跳过");
                            System.out.println(result + " ==> json→token");
                        }
                    }
                }
                //计算剩下的类型 区别accesstoken 和token 分别到request字段 or 参数字段 寻找token 参数字段优先
                else {
                    //api 寻找access_token
                    if ("api".equals(lines[3])) {
                        token = ESNStreaming2Hbase.Token(lines);
                        if (token.equals(""))
                            try {
                                if ((lines[9].split(" ").length >= 3) && (lines[9].contains("?")) && (lines[9].contains("token"))) {
                                    URL aURL = new URL("http://test.com" + lines[9].split(" ")[1]);
                                    if ((!"".equals(aURL.getQuery())) && (aURL.getQuery().contains("token"))) {
                                        System.out.print("");
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
                    }
                    //其余的字段 按照token计算
                    else {
                        token = ESNStreaming2Hbase.Token(lines);
                        if ("".equals(token)) {
                            try {
                                if ((lines[9].split(" ").length >= 3) && (lines[9].contains("?")) && (lines[9].contains("token"))) {
                                    URL aURL = new URL("http://test.com" + lines[9].split(" ")[1]);
                                    if ((!"".equals(aURL.getQuery())) && (aURL.getQuery().contains("token"))) {
                                        String[] str = aURL.getQuery().split("&");
                                        System.out.print("");
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
                        mquID = JSONUtil.getmquStr(HttpReqUtil.getResult("user/redis/api/" + token, ""));
                    }
                }
                //局域网不在统计范围内
                if (!remote_addr.contains("局域网")) {
                    String value = s + "\t" + remote_addr + "\t" + mquID;
                    String times = getTime(lines[7]);
                    //主键
                    System.out.println(times + ":" + lines[2] + ":" + lines[3] + ":" + lines[8] + "&&" + token + "==>" + lines[9]);
                    return new Tuple2<String, String>(times + ":" + lines[2] + ":" + lines[3] + ":" + lines[8], value);
                } else {
                    return null;
                }
            }
        })
                //去除对应的null
                .filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                        if (tuple2 != null) {
                            return true;
                        }
                        return false;
                    }
                });
        //开始存hbase
        modify.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                        Tuple2<String, String> tuple = null;
                        List<Put> puts = new ArrayList<Put>();
                        while (tuple2Iterator.hasNext()) {
                            tuple = tuple2Iterator.next();
                            if (tuple != null) {
                                Put put = new Put((String.valueOf(tuple._1)).getBytes());
                                put.addColumn(Bytes.toBytes("accesslog"), Bytes.toBytes("info"), Bytes.toBytes(tuple._2));
                                puts.add(put);
                            }
                        }
                        if (puts.size() > 0) {
                            HTable hTable = HbaseConnectionFactory.gethTable("esn_accesslog", "accesslog");
                            hTable.put(puts);
                            hTable.flushCommits();
                            System.out.println("hbase ==> " + puts.size());
                            puts.clear();
                            if (hTable != null) {
                                hTable.close();
                            }
                        }
                    }
                });
            }
        });
        return modify;
    }

    /**
     * opapi 计算mqui
     *
     * @param line
     * @return
     */
    private static String getOpenApi(String line) {
        //首先判断是否20字段为qz_id=80298882&instance_id=78136078&member_id=68175624类型
        String[] mqu20 = line.replace("\"","").split("&");
        String qz = "qz_id:empty";
        String ins = "instance_id:empty";
        String mem = "member_id:empty";
        //判断是否长度3 user 设置为empty
        if (mqu20.length == 3) {
            for (String mqu : mqu20) {
                String[] mqus = mqu.split("=");
                if (mqus.length == 2) {
                    String key = mqus[0];
                    String value = IdCrypt.decodeId(mqus[1]);
                    if ("qz_id".equals(key)) {
                        qz = key + ":" + value;
                    } else if ("instance_id".equals(key)) {
                        ins = key + ":" + value;
                    } else if ("member_id".equals(key)) {
                        mem = key + ":" + value;
                    }
                }
            }
            StringBuffer res = new StringBuffer();
            res.append(mem).append("\t").append(qz).append("\t").append("user_id:empty\t").append(ins);
            return res.toString();
        } else {
            return "member_id:empty\tqz_id:empty\tuser_id:empty\tinstance_id:empty";
        }
    }


    /**
     * 获取请求中的token
     *
     * @param lines
     * @return
     */
    private static String Token(String[] lines) {
        String _token = "";
        String s1 = lines[10].replace("\\x22", "\"").replace("\"", "").replace("\\x5C", "");
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

    private static String getCity(String city) {
        String str = "haiwai";//35
        if ("澳门".equals(city)) {//36
            str = "aomen";
        } else if ("香港".equals(city)) {//1
            str = "xianggang";
        } else if ("台湾".equals(city)) {//2
            str = "taiwan";
        } else if ("广东".equals(city)) {//3
            str = "guangdong";
        } else if ("广西".equals(city)) {//4
            str = "guangxi";
        } else if ("海南".equals(city)) {//5
            str = "hainan";
        } else if ("云南".equals(city)) {//6
            str = "yunnan";
        } else if ("福建".equals(city)) {//7
            str = "fujian";
        } else if ("江西".equals(city)) {//8
            str = "jiangxi";
        } else if ("湖南".equals(city)) {//9
            str = "hunan";
        } else if ("贵州".equals(city)) {//10
            str = "guizhou";
        } else if ("浙江".equals(city)) {//11
            str = "zhejiang";
        } else if ("安徽".equals(city)) {//12
            str = "anhui";
        } else if ("上海".equals(city)) {//13
            str = "shanghai";
        } else if ("江苏".equals(city)) {//14
            str = "jiangsu";
        } else if ("湖北".equals(city)) {//15
            str = "hubei";
        } else if ("西藏".equals(city)) {//16
            str = "xizang";
        } else if ("青海".equals(city)) {//17
            str = "qinghai";
        } else if ("陕西".equals(city)) {//18
            str = "shanxi_shan";
        } else if ("山西".equals(city)) {//19
            str = "shanxi_jin";
        } else if ("河南".equals(city)) {//20
            str = "henan";
        } else if ("山东".equals(city)) {//21
            str = "shandong";
        } else if ("河北".equals(city)) {//22
            str = "hebei";
        } else if ("天津".equals(city)) {//23
            str = "tianjin";
        } else if ("北京".equals(city)) {//24
            str = "beijing";
        } else if ("宁夏".equals(city)) {//25
            str = "ningxia";
        } else if ("内蒙古".equals(city)) {//26
            str = "neimeng";
        } else if ("辽宁".equals(city)) {//27
            str = "liaoning";
        } else if ("吉林".equals(city)) {//28
            str = "jilin";
        } else if ("黑龙江".equals(city)) {//29
            str = "heilongjiang";
        } else if ("重庆".equals(city)) {//30
            str = "chongqing";
        } else if ("四川".equals(city)) {//31
            str = "sichuan";
        } else if ("新疆".equals(city)) {//32
            str = "xinjiang";
        } else if ("甘肃".equals(city)) {//33
            str = "gansu";
        } else if ("局域网".equals(city)) {//34
            str = "local";
        }
        return str;
    }

    private static String getTime(String timestamp) {
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
