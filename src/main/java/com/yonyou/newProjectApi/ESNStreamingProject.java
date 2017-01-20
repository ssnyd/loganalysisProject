package com.yonyou.newProjectApi;

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
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
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
 * 优化streaming 设置缓存 优化内部处理结构
 * Created by chenxiaolei on 16/12/26.
 */
public class ESNStreamingProject {
    //设置checkpoint
    private static final String ESNSTREAMING2HBASE = "hdfs://cluster/streaming/checkpoin/syb/ESNStreaming2Hbase";
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
     * 运行主要程序代码
     *
     * @return JavaStreamingContext实例
     */
    public static JavaStreamingContext createContext() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("ESNStreamingProject")
                .set("spark.default.parallelism", "15")//並行度，reparation后生效(因为集群现在的配置是8核，按照每个核心有一个vcore，就是16，三个worker节点，就是16*3，并行度设置为3倍的话：16*3*3=144，故，这里设置150)
                .set("spark.locality.wait", "100ms")
                .set("spark.shuffle.manager", "hash")//使用hash的shufflemanager
                .set("spark.shuffle.consolidateFiles", "true")//shufflemap端开启合并较小落地文件（hashshufflemanager方式一个task对应一个文件，开启合并，reduce端有几个就是固定几个文件，提前分配好省着merge了）
                .set("spark.storage.memoryFraction", "0.7")
                .set("spark.shuffle.file.buffer", "64")//shufflemap端mini环形缓冲区bucket的大小调大一倍，默认32KB
                .set("spark.reducer.maxSizeInFlight", "24")//从shufflemap端拉取数据24，默认48M
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//序列化
                .set("spark.shuffle.io.maxRetries", "20")//GC重试次数，默认3
                .set("spark.shuffle.io.retryWait", "60s")//GC等待时长，默认5s
                .set("spark.cleaner.ttl","43200")
                ;
        //设置批次时间 5s
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(ESNSTREAMING2HBASE_TIME));
        //设置spark容错点
        jssc.checkpoint(ESNSTREAMING2HBASE);
        ////创建一个工厂 获取存储区域及4个ID
        //GroupCacheFactory factory = new GroupCacheFactory();
        ////******************************************************************
        ////获取一个组
        //Group hahaarea=factory.group("area");
        ////JavaSparkContext javaSparkContext = jssc.sparkContext();
        //final Broadcast<Group> areaBro = jssc.sparkContext().broadcast(hahaarea);
        //*******************************************************************
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
                    private static final long serialVersionUID = 3461702229356118993L;

                    public String call(MessageAndMetadata<String, String> v1)
                            throws Exception {
                        return v1.message();
                    }
                });
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        final JavaDStream<String> line = lines.transform(
                new Function<JavaRDD<String>, JavaRDD<String>>() {
                    private static final long serialVersionUID = -3451503555864967626L;

                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);
                        return rdd;
                    }
                }
        );
               // .repartition(20);
        JavaDStream<String> line2map = line.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>((int) (1 + Math.random() * (3 - 1 + 1)), s);
            }
        }).repartition(20).map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> v1) throws Exception {
                return v1._2;
            }
        });
        //&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // 1 过滤数据源
        JavaDStream<String> filter = filter(line2map);
        // 2 添加区域字段
        JavaDStream<String> modifyRDD2Area = modifyRDD2Area(filter);
        //增加mqui字段
        JavaDStream<String> modifyRDD2Mqui = modifyRDD2Mqui(modifyRDD2Area);
        JavaPairDStream<String, String> resultRDD = modifyRDD2Pair(modifyRDD2Mqui);
        //缓存数据源
        resultRDD = resultRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
        //计算pv
        JavaPairDStream<String, String> PVStat = calculatePVSta(resultRDD);
        //存到hbase
        reslut2hbase(resultRDD);
        ///&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //存储zookeeper offset
        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            private static final long serialVersionUID = 259366885091079449L;

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

    private static void reslut2hbase(JavaPairDStream<String, String> resultRDD) {
        //开始存hbase
        resultRDD.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                //修改的地方 0110
                rdd.coalesce(1).foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
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
                        //0110
                        System.out.println("hbase size " + puts.size() + DateUtils.gettest());
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
    }


    /**
     * 增加区域字段
     *
     * @param filter
     * @return
     */
    private static JavaDStream<String> modifyRDD2Area(JavaDStream<String> filter) {

        return filter.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            private static final long serialVersionUID = 6424299360179888036L;

            @Override
            public Iterable<String> call(Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                Map<String, String> ipmap = new HashMap<String, String>();
                String remote_addr = "country:empty\tregion:empty\tcity:empty";
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] lines = line.split("\t");
                    //获取ip
                    String ip = lines[0];
                    //是否截取成功
                    if (!"".equals(ip) && ip.length() < 16) {
                        try {
                            if (ipmap.get(ip) == null || "".equals(ipmap.get(ip))) {
                                //通过接口获取
                                remote_addr = JSONUtil.getIPStr(HttpReqUtil.getResult("ip/query", ip));
                                ipmap.put(ip, remote_addr);
                            } else {
                                remote_addr = ipmap.get(ip);
                                if (ipmap.size() > 100) {
                                    ipmap.clear();
                                }
                            }
                        } catch (Exception e) {
                            System.out.println(ip + " ==> read time out! 数据跳过");
                        }
                    }
                    //局域网不在统计范围内
                    if (!remote_addr.contains("局域网")) {
                        //if() 外面添加到list
                        list.add(line + "\t" + remote_addr);
                    }
                }
                ipmap.clear();
                return list;
            }
        });
    }

    /**
     * 添加4个id字段 先缓存 没有 就去找接口
     *
     * @param modifyRDD2Area
     * @return
     */
    private static JavaDStream<String> modifyRDD2Mqui(JavaDStream<String> modifyRDD2Area) {
        return modifyRDD2Area.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            private static final long serialVersionUID = 89196058162062616L;

            @Override
            public Iterable<String> call(Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                String mquID = "member_id:empty\tqz_id:empty\tuser_id:empty\tinstance_id:empty";
                //token和sessionid对应的qzid会发生变化
                //Map<String, String> mquimap = new HashMap<String, String>();
                //存储token
                String token = "";
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] lines = line.split("\t");
                    //求mqu
                    //遇见.htm请求的不分析直接存空
                    if (lines[9].contains(".htm")) {
                        System.out.println("存在htm 不分析 ==> " + lines[9]);
                    }
                    //openapi 最后 20位 字段qz_id=80298882&instance_id=78136078&member_id=68175624 需要反编译
                    else if ("openapi".equals(lines[3])) {
                        mquID = getOpenApi(lines[19]);
                    }
                    //针对esn处理 找到PHPSESSID 调用user/redis/esn/"
                    else if ("esn".equals(lines[3])) {
                        if (lines[19].split(" ")[0].contains("PHPSESSID")) {
                            token = lines[19].split(" ")[0].split("=")[1].split(";")[0];
                        }
                        if (!"".equals(token) && !token.contains("/") && !token.contains("\\")) {
                            try {
                                mquID = JSONUtil.getmquStr(HttpReqUtil.getResult("user/redis/esn/" + token, ""));
                            } catch (Exception e) {
                                System.out.println(token + "read time out ! token数据跳过");
                                System.out.println(token + " ==> json→token");
                            }
                        }
                    } else {
                        //api 寻找access_token
                        if ("api".equals(lines[3])) {
                            token = Token(lines);
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
                            token = Token(lines);
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
                            try {
                                mquID = JSONUtil.getmquStr(HttpReqUtil.getResult("user/redis/api/" + token, ""));
                            } catch (Exception e) {
                                System.out.println(token + "read time out ! token数据跳过");
                                System.out.println(token + " ==> json→token");
                            }
                        }
                    }
                    list.add(line + "\t" + mquID);
                }
                return list;
            }
        });
    }


    /**
     * 组合成tuple
     *
     * @param modifyRDD2Mqui
     * @return
     */
    private static JavaPairDStream<String, String> modifyRDD2Pair(JavaDStream<String> modifyRDD2Mqui) {
        return modifyRDD2Mqui.mapToPair(new PairFunction<String, String, String>() {
            private static final long serialVersionUID = -6654397016059041309L;

            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] lines = s.split("\t");
                String key = getTime(lines[7]) + ":" + lines[2] + ":" + lines[3] + ":" + UUID.randomUUID().toString().replace("-", "");
                //0110
                System.out.println(key + "==>" + lines[9]);
                return new Tuple2<String, String>(key, s);
            }
        });
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
            private static final long serialVersionUID = -4877186775851329290L;

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
     * opapi 计算mqui
     *
     * @param line
     * @return
     */
    private static String getOpenApi(String line) {
        //首先判断是否20字段为qz_id=80298882&instance_id=78136078&member_id=68175624类型
        String[] mqu20 = line.replace("\"", "").split("&");
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

    /**
     * PV to mysql
     *
     * @param modifyLogDStream
     * @return
     */
    private static JavaPairDStream<String, String> calculatePVSta(JavaPairDStream<String, String> modifyLogDStream) {
        JavaPairDStream<String, String> filterDStream = modifyLogDStream.filter(new Function<Tuple2<String, String>, Boolean>() {
            private static final long serialVersionUID = 6482360183531301253L;

            @Override
            public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                if (tuple2 != null && !tuple2._1.contains("openapi")) {
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
            private static final long serialVersionUID = 8531536342057775128L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, 1);
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
            private static final long serialVersionUID = -4468385697813777397L;

            @Override
            public String call(String s, String s2) throws Exception {
                return s + "#" + s2;
            }
        }, 1);
        maptopair.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            private static final long serialVersionUID = -1293890758683239645L;

            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                rdd.coalesce(1).foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                    private static final long serialVersionUID = -872354222578302313L;

                    @Override
                    public void call(Iterator<Tuple2<String, String>> tuples) throws Exception {
                        List<Map<String, String>> pvStats = new ArrayList<Map<String, String>>();
                        Tuple2<String, String> tuple = null;
                        String[] region_count = null;
                        while (tuples.hasNext()) {
                            Map<String, String> pvStat = new HashMap<String, String>();
                            tuple = tuples.next();
                            region_count = tuple._2.split("#");
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
                        try {
                            //0110
                            System.out.println("mysql size" + pvStats.size() + DateUtils.gettest());
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
                        } catch (Exception e) {
                            System.out.println("出现错误啦");
                            e.printStackTrace();
                        }
                    }
                });
            }
        });


        //maptopair.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
        //    private static final long serialVersionUID = 1L;
        //
        //    @Override
        //    public Void call(JavaPairRDD<String, String> rdd) throws Exception {
        //        //这里有改动 0110
        //        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
        //            private static final long serialVersionUID = -872354222578302313L;
        //
        //            @Override
        //            public void call(Iterator<Tuple2<String, String>> tuples) throws Exception {
        //                List<Map<String, String>> pvStats = new ArrayList<Map<String, String>>();
        //                Tuple2<String, String> tuple = null;
        //                String[] region_count = null;
        //                while (tuples.hasNext()) {
        //                    Map<String, String> pvStat = new HashMap<String, String>();
        //                    tuple = tuples.next();
        //                    region_count = tuple._2.split("#");
        //                    pvStat.put("timestamp", tuple._1);
        //                    for (String str : region_count) {
        //                        String[] s = str.split("&");
        //                        if (pvStat.get(s[0]) != null) {
        //                            int num = Integer.parseInt(pvStat.get(s[0])) + Integer.parseInt(s[1]);
        //                            pvStat.put(s[0], num + "");
        //                        } else {
        //                            pvStat.put(s[0], s[1]);
        //                        }
        //                    }
        //                    pvStats.add(pvStat);
        //                }
        //                try{
        //                    //0110
        //                    System.out.println("mysql size"+pvStats.size()+DateUtils.gettest());
        //                    if (pvStats.size() > 0) {
        //                        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        //                        Connection conn = jdbcUtils.getConnection();
        //                        ILogStatDAO logStatDAO = DAOFactory.getLogStatDAO();
        //                        logStatDAO.updataBatch(pvStats, conn);
        //                        System.out.println("mysql  to pvstat ==> " + pvStats.size());
        //                        pvStats.clear();
        //                        if (conn != null) {
        //                            jdbcUtils.closeConnection(conn);
        //                        }
        //                    }
        //                } catch (Exception e){
        //                    System.out.println("出现错误啦");
        //                    e.printStackTrace();
        //                }
        //            }
        //        });
        //        return null;
        //    }
        //});
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
}


