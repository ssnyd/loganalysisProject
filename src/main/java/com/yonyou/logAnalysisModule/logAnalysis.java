package com.yonyou.logAnalysisModule;

import com.google.common.base.Optional;
import com.yonyou.conf.ConfigurationManager;
import com.yonyou.constant.Constants;
import com.yonyou.dao.IMemIdStatDAO;
import com.yonyou.dao.ILogStatDAO;
import com.yonyou.dao.IUVStatDAO;
import com.yonyou.dao.factory.DAOFactory;
import com.yonyou.dao.IIPVStatDAO;
import com.yonyou.entity.IPVStat;
import com.yonyou.entity.MemIDStat;
import com.yonyou.entity.UVStat;
import com.yonyou.hbaseUtil.HbaseConnectionFactory;
import com.yonyou.jdbc.JDBCUtils;
import com.yonyou.utils.HttpReqUtil;
import com.yonyou.utils.JSONUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * spark streaming 批次处理5秒数据 存hbase 及pv按秒级→mysql
 * Created by ChenXiaoLei on 2016/11/6.
 */
public class logAnalysis {
     public static void main(String[] args) {
        JavaStreamingContextFactory sparkfactory = new JavaStreamingContextFactory() {
            public JavaStreamingContext create() {
                return createContext();
            }
        };
        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(ConfigurationManager.getProperty(Constants.STREAMING_CHECKPOINT), sparkfactory);
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
     public static JavaStreamingContext createContext(){
        SparkConf conf = new SparkConf()
                .setAppName("logAnalysisStatSpark")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.streaming.blockInterval", "100")//ms→RDD
//              .set("spark.streaming.unpersist", "true")
                .set("spark.shuffle.io.maxRetries", "60")
                .set("spark.shuffle.io.retryWait", "60s")
                .set("spark.reducer.maxSizeInFlight", "24")
                .set("spark.streaming.receiver.writeAheadLog.enable", "true");
//        conf.setMaster("local[2]");//本地测试
        JavaStreamingContext jsc = new JavaStreamingContext(
                conf, Durations.seconds(5));
        //设置spark容错点
        jsc.checkpoint(ConfigurationManager.getProperty(Constants.STREAMING_CHECKPOINT));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Set<String> topics = new HashSet<String>();
        for(String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }//com.yonyou.logAnalysisModule.logAnalysis
        JavaPairInputDStream<String, String> logDStream = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
         JavaPairDStream<String, String> persist = logDStream.persist(StorageLevel.MEMORY_AND_DISK());
         // 进行数据过滤
        JavaPairDStream<String,String> filterLogDStream =
                filterByRequest(persist);
        // 经行数据转换 增加区域字段 3个ID
        JavaPairDStream<String,String> modifyLogDStream = modifyByIPAndToken(filterLogDStream);
        modifyLogDStream = modifyLogDStream.persist(StorageLevel.MEMORY_AND_DISK());
        //PV
        JavaPairDStream<String, String> PVLogDStream = calculatePVSta(modifyLogDStream);
        //UV
        JavaPairDStream<String, Integer> UVLogDStream = calculateUVSta(modifyLogDStream);
        JavaPairDStream<String, Integer> IPVLogDStream = calculateIPVSta(modifyLogDStream);
        return jsc;
    }

    /**
     * 计算ipv 每小时
     * @param modifyLogDStream
     * @return
     */
    private static JavaPairDStream<String,Integer> calculateIPVSta(JavaPairDStream<String, String> modifyLogDStream) {
        final JavaPairDStream<String, Integer> mapPairRDD = modifyLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                String log = tuple2._2;
                String[] logSplited = log.split("\t");
                long timestamp = getTime(logSplited[7], 2);//每小时的时间戳
                String ip = logSplited[0];
                return new Tuple2<String, Integer>(timestamp + "&" + ip, 1);
            }
        }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> optional) throws Exception {
                Integer clickCount = 0;
                if (optional.isPresent()) {
                    clickCount = optional.get();
                }
                for (Integer value : values) {
                    clickCount += value;
                }
                return Optional.of(clickCount);
            }
        });
        mapPairRDD.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> pair) throws Exception {
                JavaPairRDD<String, String> mapRDD = pair.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                        String time = tuple2._1.split("&")[0];
                        String ip = tuple2._1.split("&")[1];
                        return new Tuple2<String, String>(time, ip);
                    }
                });
                JavaPairRDD<String, Integer> union = mapRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                        String time = tuple2._1;
                        long hour = Long.parseLong(time);
                        long dayTime = getDayLong(hour);
                        return new Tuple2<String, String>(dayTime + "day", tuple2._2);
                    }
                }).distinct().mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                        return new Tuple2<String, Integer>(tuple2._1, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }).union(mapRDD.distinct().mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                        return new Tuple2<String, Integer>(tuple2._1 + "hour", 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }));
                return union;
            }
        }).foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> tuple2) throws Exception {
                        List<IPVStat> ipvStats = new ArrayList<IPVStat>();
                        Tuple2<String, Integer> tuple = null;
                        while (tuple2.hasNext()) {
                            tuple = tuple2.next();
                            String _created = tuple._1;
                            String created = _created.substring(0,_created.length()-4);
                            String type = "1hour";
                            if(_created.contains("day")){
                                created = _created.substring(0,_created.length()-3);
                                type = "1day";
                            }
                            String clientType = "all";
                            Integer num = tuple._2;
                            IPVStat ipvStat = new IPVStat();
                            ipvStat.setClientType(clientType);
                            ipvStat.setType(type);
                            ipvStat.setCreated(created);
                            ipvStat.setNum(num);
                            ipvStats.add(ipvStat);

                        }
                        if (ipvStats.size()>0){
                            JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                            Connection conn = jdbcUtils.getConnection();
                            IIPVStatDAO ipvStatDAO = DAOFactory.getIPVStatDAO();
                            ipvStatDAO.updataBatch(ipvStats,conn);
                            System.out.println("mysql to ipvstat ==> "+ipvStats.size());
                            ipvStats.clear();
                            if (conn != null) {
                                jdbcUtils.closeConnection(conn);
                            }
                        }
                    }
                });
            }
        });
        return null;
    }
    /**
     * UV计算 按时按天 13:00 13:59 ==> key 13:00
     * @param modifyLogDStream
     * @return
     */
    private static JavaPairDStream<String,Integer> calculateUVSta(JavaPairDStream<String, String> modifyLogDStream) {
        JavaPairDStream<String, Integer> mapPairDStream = modifyLogDStream.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                return v1._2.split("\t").length ==26 && v1._2.split("\t")[24].split(":").length ==2 ;
            }
        }).mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                String log = tuple2._2;
                String[] logSplited = log.split("\t");
                long timestamp = getTime(logSplited[7], 2);//获得每小时的时间戳
                String memberId = logSplited[23].split(":")[1];//获得memid
                String qzid = logSplited[24].split(":")[1];//获得qzid
//              String memberId = new Random().nextInt(3)+1+"";
                return new Tuple2<String, Integer>(timestamp + "&" + memberId+"&"+qzid, 1);
            }
        }).filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> tuple2) throws Exception {
                return !tuple2._1.split("&")[1].equals("empty");
            }
        });
        JavaPairDStream<String, Integer> updatePairDStream = mapPairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> optional) throws Exception {
                Integer clickCount = 0;
                if (optional.isPresent()) {
                    clickCount = optional.get();
                }
                for (Integer value : values) {
                    clickCount += value;
                }
                return Optional.of(clickCount);
            }
        });

        updatePairDStream.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> pair) throws Exception {
                JavaPairRDD<String, String> mapRDD = pair.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple2<String, String>(tuple2._1.split("&")[0], tuple2._1.split("&")[1] + "&" + tuple2._1.split("&")[2]);
                    }
                });
                JavaPairRDD<String, Integer> union = mapRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                        String time = tuple2._1;
                        long hour = Long.parseLong(time);
                        long dayTime = getDayLong(hour);//转换成天的 每天的0:00 的时间戳
                        return new Tuple2<String, String>(dayTime + "1day", tuple2._2);
                    }
                }).distinct().mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                        return new Tuple2<String, Integer>(tuple2._1, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }).union(mapRDD.distinct().mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                        return new Tuple2<String, Integer>(tuple2._1 + "1hour", 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }));
                return union;
            }
        }).foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> tuple2) throws Exception {
                        List<UVStat> uvStats = new ArrayList<UVStat>();
                        Tuple2<String, Integer> tuple = null;
                        while (tuple2.hasNext()) {
                            tuple = tuple2.next();
                            String _created = tuple._1;
                            String created = _created.substring(0,_created.length()-5);
                            String type = "1hour";
                            if(_created.contains("1day")){
                                created = _created.substring(0,_created.length()-4);
                                type = "1day";
                            }
                            String clientType = "all";
                            Integer num = tuple._2;
                            UVStat uvStat = new UVStat();
                            uvStat.setType(type);
                            uvStat.setClientType(clientType);
                            uvStat.setCreated(created);
                            uvStat.setNum(num);
                            uvStats.add(uvStat);
                        }
                        if (uvStats.size()>0){
                            JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                            Connection conn = jdbcUtils.getConnection();
                            IUVStatDAO uvStatDAO = DAOFactory.getUVStatDAO();
                            uvStatDAO.updataBatch(uvStats,conn);
                            System.out.println("mysql to uvstat ==> "+uvStats.size());
                            uvStats.clear();
                            if (conn != null) {
                                jdbcUtils.closeConnection(conn);
                            }
                        }
                    }
                });
            }
        });

        //计算mem
//        updatePairDStream.mapToPair(new PairFunction<Tuple2<String,Integer>, String,Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
//                String time = tuple2._1.split("&")[0];
//                long hour = Long.parseLong(time);
//                long dayTime = getDayLong(hour);
//                return new Tuple2<String, Integer>(dayTime + "&"+tuple2._1.split("&")[1]+ "&"+tuple2._1.split("&")[2], tuple2._2);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        }).foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
//            @Override
//            public Void call(JavaPairRDD<String, Integer> rdd) throws Exception {
//                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
//                    @Override
//                    public void call(Iterator<Tuple2<String, Integer>> tuple2) throws Exception {
//                        List<MemIDStat> memIDStats = new ArrayList<MemIDStat>();
//                        Tuple2<String, Integer> tuple = null;
//                        while (tuple2.hasNext()) {
//                            tuple = tuple2.next();
//                            MemIDStat memIDStat = new MemIDStat();
//                            memIDStat.setCreated(tuple._1.split("&")[0]);
//                            memIDStat.setMemberId(tuple._1.split("&")[1]);
//                            memIDStat.setQzId(tuple._1.split("&")[2]);
//                            memIDStat.setTimes(tuple._2);
//                            memIDStats.add(memIDStat);
//                        }
//                        if (memIDStats.size()>0){
//                            JDBCUtils jdbcUtils = JDBCUtils.getInstance();
//                            Connection conn = jdbcUtils.getConnection();
//                            IMemIdStatDAO menIdStatDAO = DAOFactory.getMemIdStatDAO();
//                            menIdStatDAO.updataBatch(memIDStats,conn);
//                            System.out.println("mysql to memstat - day==> "+ memIDStats.size());
//                            memIDStats.clear();
//                            if (conn != null) {
//                                jdbcUtils.closeConnection(conn);
//                            }
//                        }
//                    }
//                });
//                return null;
//            }
//        });
        return null;
    }


    /**
     *PV to mysql
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
                long timestamp = getTime(logSplited[7],0);
                String region = getCity(logSplited[21].split(":")[1]);
                String key = timestamp + "%" + region;
                return new Tuple2<String, Integer>(key, 1);

            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
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
                        List<Map<String,String>> pvStats = new ArrayList<Map<String,String>>();
                        Tuple2<String, String> tuple = null;
                        while (tuples.hasNext()) {
                            Map<String, String> pvStat = new HashMap<String, String>();
                            tuple = tuples.next();
                            String[] region_count = tuple._2.split("#");
                            pvStat.put("timestamp", tuple._1);
                            for (String str : region_count) {
                                String[] s = str.split("&");
                                if (pvStat.get(s[0])!=null){
                                    int num = Integer.parseInt(pvStat.get(s[0])) + Integer.parseInt(s[1]);
                                    pvStat.put(s[0],num+"");
                                }else {
                                    pvStat.put(s[0], s[1]);
                                }
                            }
                            pvStats.add(pvStat);
                        }
                        if (pvStats.size()>0){
                            JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                            Connection conn = jdbcUtils.getConnection();
                            ILogStatDAO logStatDAO = DAOFactory.getLogStatDAO();
                            logStatDAO.updataBatch(pvStats,conn);
                            System.out.println("mysql  to pvstat==> "+pvStats.size() );
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
        return maptopair ;
    }
    /**
     * 增加新的字段 地址加3ID \t 拼接原始数据
     * @param filterLogDStream
     * @return
     */
    private static JavaPairDStream<String,String> modifyByIPAndToken(JavaPairDStream<String, String> filterLogDStream) {
        JavaPairDStream<String, String> mfJavaPairDStream = filterLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                        String token = "";
                        String[] lines = tuple._2.split("\t");
                        String mquID = "member_id:empty\tqz_id:empty\tuser_id:empty";
                        String remote_addr = "country:empty\tregion:empty\tcity:empty";
                        //求ip
                        String ip = lines[0];
                        if (!"".equals(ip) && ip.length()<16){
                            String result = "";
                            try {
                                result = HttpReqUtil.getResult("ip/query",ip);
                                try {
                                    remote_addr = JSONUtil.getIPStr(result);
                                }catch (Exception e){
                                    System.out.println(result+" ==> 解析ip→json错误");
                                    System.out.println(ip+" ==> json→ip");
                                }
                            }catch (Exception e){
                                System.out.println(ip+" ==> read time out! 数据跳过");
                            }
                        }
                        if (tuple._2.contains(".htm")) {
                            mquID = "member_id:empty\tqz_id:empty\tuser_id:empty";
                        } else if ("esn".equals(lines[3])) {
                            if (lines[19].split(" ")[0].contains("PHPSESSID")) {
                                token = lines[19].split(" ")[0].split("=")[1].split(";")[0];
                            }
                            if (!"".equals(token)&& !token.contains("/") && !token.contains("\\")){
                                String result = "";
                                try {
                                    result = HttpReqUtil.getResult("user/redis/esn/" + token, "");
                                    mquID = JSONUtil.getmquStr(result);
                                } catch (Exception e ){
                                    System.out.println( token+"read time out! token数据跳过");
                                    System.out.println(result + " ==> json→token");
                                }
                            }
                        } else {
                            if ("api".equals(lines[3])) {
                                token = logAnalysis.Token(lines);
                                if (token.equals(""))
                                    try {
                                        if ((lines[9].split(" ").length >= 3) && (lines[9].contains("?")) && (lines[9].contains("token"))) {
                                            URL aURL = new URL("http://test.com" + lines[9].split(" ")[1]);
                                            if ((!"".equals(aURL.getQuery())) && (aURL.getQuery().contains("token"))) {
                                                String[] str = aURL.getQuery().split("&");
                                                for (int i = 0; i < str.length; i++)
                                                    if ("access_token".equals(str[i].split("=")[0]) && str[i].split("=").length==2) {
                                                        token = str[i].split("=")[1];
                                                        break;
                                                    }
                                            }
                                        }
                                    }
                                    catch (MalformedURLException e) {
                                        e.printStackTrace();
                                    }
                            }
                            else {
                                token = logAnalysis.Token(lines);
                                if ("".equals(token)) {
                                    try {
                                        if ((lines[9].split(" ").length >= 3) && (lines[9].contains("?")) && (lines[9].contains("token"))) {
                                            URL aURL = new URL("http://test.com" + lines[9].split(" ")[1]);
                                            if ((!"".equals(aURL.getQuery())) && (aURL.getQuery().contains("token"))) {
                                                String[] str = aURL.getQuery().split("&");
                                                for (int i = 0; i < str.length; i++)
                                                    if ("token".equals(str[i].split("=")[0]) && str[i].split("=").length==2 ) {
                                                        token = str[i].split("=")[1];
                                                        break;
                                                    }
                                            }
                                        }
                                    }
                                    catch (MalformedURLException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                            if (!"".equals(token) && !token.contains("/") && !token.contains("\\")){
                                mquID = JSONUtil.getmquStr(HttpReqUtil.getResult("user/redis/api/" + token, ""));
                            }
                        }
                        if (!remote_addr.contains("局域网")){
                            String value = tuple._2 + "\t" + remote_addr + "\t" + mquID;
                            System.out.println(lines[2] + ":" + lines[3] + ":" + lines[8] + "&&" + token + "==>" + lines[9]);
                            return new Tuple2<String, String>(lines[2] + ":" + lines[3] + ":" + lines[8], value);
                        }else {
                            return null;
                        }
                    }
                }
        ).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                if (tuple2 != null) {
                    return true;
                }
                return false;
            }
        });

        mfJavaPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                        Tuple2<String, String> tuple = null;
                        List<Put> puts = new ArrayList<Put>();
                        while (tuple2Iterator.hasNext()) {
                            tuple = tuple2Iterator.next();
                            if(tuple!=null){
                                Put put = new Put((String.valueOf(tuple._1)).getBytes());
                                put.addColumn(Bytes.toBytes("accesslog"),Bytes.toBytes("log"),Bytes.toBytes(tuple._2));
                                puts.add(put);
                            }
                        }
                        if (puts.size()>0){
                            HTable hTable = HbaseConnectionFactory.gethTable("esn", "accesslog");
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
        return mfJavaPairDStream;
    }
    /**
     * 获取请求中的token
     * @param lines
     * @return
     */
    private static String Token(String[] lines) {
        String _token = "";
        String s1 = lines[10].replace("\\x22", "\"").replace("\"","").replace("\\x5C", "");
        String[] split = s1.split(",");
        for (int i =0 ;i<split.length;i++){
            if(split[i].contains("token")){
                if (split[i].split(":").length==2)
                    _token = split[i].split(":")[1].replace("}","").replace("{","");
                break;
            }
        }
        return _token;
    }

    /**
     * filterData
     * @param logDStream
     * @return
     */
    private static JavaPairDStream<String,String> filterByRequest(JavaPairDStream<String,String> logDStream) {
        return logDStream.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        String[] log = tuple._2.split("\t");
                        boolean flag  = false;
                        if (log.length == 20 && "esn".equals(log[3])){
                            flag = log[19].contains("PHPSESSID");
                        }else if(log.length == 20 && ("api".equals(log[3]) || "h5-api".equals(log[3]) || "pc-api".equals(log[3]) || "m".equals(log[3]))){
                            flag = tuple._2.contains("token");
                        }
                        return (tuple._2.contains(".htm") || ((!(tuple._2.contains(".js ")||tuple._2.contains(".css")||tuple._2.contains(".js?")||tuple._2.contains(".png")||tuple._2.contains(".gif"))) && flag && 1==1));
                    }
                }
        );
    }
    private static String getCity(String city){
        String str = "haiwai";
        if ("澳门".equals(city)) {
            str = "aomen";
        }else if ("香港".equals(city)) {
            str = "xianggang";
        }else if ("台湾".equals(city)) {
            str = "taiwan";
        }else if ("广东".equals(city)) {
            str = "guangdong";
        }else if ("广西".equals(city)) {
            str = "guangxi";
        }else if ("海南".equals(city)) {
            str = "hainan";
        }else if ("云南".equals(city)) {
            str = "yunnan";
        }else if ("福建".equals(city)) {
            str = "fujian";
        }else if ("江西".equals(city)) {
            str = "jiangxi";
        }else if ("湖南".equals(city)) {
            str = "hunan";
        }else if ("贵州".equals(city)) {
            str = "guizhou";
        }else if ("浙江".equals(city)) {
            str = "zhejiang";
        }else if ("安徽".equals(city)) {
            str = "anhui";
        }else if ("上海".equals(city)) {
            str = "shanghai";
        }else if ("江苏".equals(city)) {
            str = "jiangsu";
        }else if ("湖北".equals(city)) {
            str = "hubei";
        }else if ("西藏".equals(city)) {
            str = "xizang";
        }else if ("青海".equals(city)) {
            str = "qinghai";
        }else if ("陕西".equals(city)) {
            str = "shanxi_shan";
        }else if ("山西".equals(city)) {
            str = "shanxi_jin";
        }else if ("河南".equals(city)) {
            str = "henan";
        }else if ("山东".equals(city)) {
            str = "shandong";
        }else if ("河北".equals(city)) {
            str = "hebei";
        }else if ("天津".equals(city)) {
            str = "tianjin";
        }else if ("北京".equals(city)) {
            str = "beijing";
        }else if ("宁夏".equals(city)) {
            str = "ningxia";
        }else if ("内蒙古".equals(city)) {
            str = "neimeng";
        }else if ("辽宁".equals(city)) {
            str = "liaoning";
        }else if ("吉林".equals(city)) {
            str = "jilin";
        }else if ("黑龙江".equals(city)) {
            str = "heilongjiang";
        }else if ("重庆".equals(city)) {
            str = "chongqing";
        }else if ("西川".equals(city)) {
            str = "sichuan";
        }else if ("局域网".equals(city)) {
            str = "local";
        }
        return str;
    }
    private static long getTime(String timestamp , int num ) {
        String strDateTime = timestamp.replace("[", "").replace("]", "");
        long datekey = 0l;
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat day = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd HH");
        Date t = null;
        String format = "";
        try {
            t = formatter.parse(strDateTime);
            if (1 == num ){
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
    private static long getDayLong(long time) {
        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd");
        long day =0l;
        try {
            String format = hour.format(time*1000);
            Date date = new Date(time * 1000);

            Date parse = hour.parse(format);
            day = parse.getTime() / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return day;
    }
}
















//
//package com.yonyou.logAnalysisModule;
//
//import com.google.common.base.*;
//import com.google.common.base.Optional;
//import com.yonyou.conf.ConfigurationManager;
//import com.yonyou.constant.Constants;
//import com.yonyou.dao.ILogStatDAO;
//import com.yonyou.dao.factory.DAOFactory;
//import com.yonyou.hbaseUtil.HbaseConnectionFactory;
//import com.yonyou.jdbc.JDBCUtils;
//import com.yonyou.utils.HttpReqUtil;
//import com.yonyou.utils.JSONUtil;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.storage.StorageLevel;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import kafka.serializer.StringDecoder;
//import scala.Tuple2;
//
//import java.sql.Connection;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.*;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
///**
// * spark streaming 批次处理5秒数据 存hbase 及pv按秒级→mysql
// * Created by ChenXiaoLei on 2016/11/6.
// */
//public class logAnalysis {
//    public static void main(String[] args) {
//        SparkConf conf = new SparkConf()
//                .setAppName("logAnalysisStatSpark")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .set("spark.streaming.blockInterval", "100")//ms→RDD
//                .set("spark.streaming.unpersist", "true")
//                .set("spark.shuffle.io.maxRetries", "60")
//                .set("spark.shuffle.io.retryWait", "60s")
//                .set("spark.reducer.maxSizeInFlight", "12")
//                .set("spark.streaming.receiver.writeAheadLog.enable", "true");
////        conf.setMaster("local[2]");//本地测试
//        JavaStreamingContext jssc = new JavaStreamingContext(
//                conf, Durations.seconds(3));
//        //设置spark容错点
//        jssc.checkpoint(ConfigurationManager.getProperty(Constants.STREAMING_CHECKPOINT));
//        List<Map<String,String>> broadCast = new ArrayList<Map<String,String>>();
//        Map<String, String> kafkaParams = new HashMap<String, String>();
//        kafkaParams.put("metadata.broker.list",
//                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
//        // 构建topic set
//        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
//        String[] kafkaTopicsSplited = kafkaTopics.split(",");
//        Set<String> topics = new HashSet<String>();
//        for(String kafkaTopic : kafkaTopicsSplited) {
//            topics.add(kafkaTopic);
//        }//com.yonyou.logAnalysisModule.logAnalysis
//        JavaPairInputDStream<String, String> logDStream = KafkaUtils.createDirectStream(
//                jssc,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                kafkaParams,
//                topics);
//        // 进行数据过滤
//        JavaPairDStream<String,String> filterLogDStream =
//                filterByRequest(logDStream);
//        // 经行数据转换 增加区域字段 3个ID
//        JavaPairDStream<String,String> modifyLogDStream = modifyByIPAndToken(filterLogDStream);
//        modifyLogDStream = modifyLogDStream.persist(StorageLevel.MEMORY_AND_DISK());
//        //PV
//        JavaPairDStream<String, String> PVLogDStream = calculatePVSta(modifyLogDStream);
//        //UV
//        //JavaPairDStream<String, Integer> UVLogDStream = calculateUVSta(modifyLogDStream);
//        modifyLogDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
//            @Override
//            public void call(JavaPairRDD<String, String> rdd) throws Exception {
//                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
//                    @Override
//                    public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
//                        Tuple2<String, String> tuple = null;
//                        List<Put> puts = new ArrayList<Put>();
//                        while (tuple2Iterator.hasNext()) {
//                            tuple = tuple2Iterator.next();
//                            if(tuple!=null){
//                                Put put = new Put((String.valueOf(tuple._1)).getBytes());
//                                put.addColumn(Bytes.toBytes("accesslog"),Bytes.toBytes("log"),Bytes.toBytes(tuple._2));
//                                puts.add(put);
//                            }
//                        }
//                        if (puts.size()>0){
//                            HTable hTable = HbaseConnectionFactory.gethTable("esn", "accesslog");
//                            hTable.put(puts);
//                            hTable.flushCommits();
//                            System.out.println("hbase ==> "+puts.size());
//                            puts.clear();
//                            if (hTable!=null){
//                                hTable.close();
//                            }
//                        }
//                    }
//                });
//            }
//        });
//        jssc.start();
//        jssc.awaitTermination();
//        jssc.close();
//    }
//
//    /**
//     * UV计算 按时按天 13:00 13:59 ==> key 13:00
//     * @param modifyLogDStream
//     * @return
//     */
//    private static JavaPairDStream<String,Integer> calculateUVSta(JavaPairDStream<String, String> modifyLogDStream) {
//        JavaPairDStream<String, Integer> mapPairDStream = modifyLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
//                String log = tuple2._2;
//                String[] logSplited = log.split("\t");
//                long timestamp = getTime(logSplited[7], 2);
//                String userid = getCity(logSplited[21].split(":")[1]);
//                return new Tuple2<String, Integer>(timestamp + "&" + userid, 1);
//            }
//        });
//        JavaPairDStream<String, Integer> userclicktohour = mapPairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer i1, Integer i2) throws Exception {
//                return i1 + i2;
//            }
//        }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
//            @Override
//            public Optional<Integer> call(List<Integer> values, Optional<Integer> optional) throws Exception {
//                int clickCount = 0;
//                if (optional.isPresent()) {
//                    clickCount = optional.get();
//                }
//                for (Integer value : values) {
//                    clickCount += value;
//                }
//                return Optional.of(clickCount);
//            }
//        });
//        //=====================================
//        //userclicktohour.foreachRDD 存到mysql
//        //=====================================
//        userclicktohour.map(new Function<Tuple2<String,Integer>, String>() {
//            @Override
//            public String call(Tuple2<String, Integer> tuple2) throws Exception {
//                return tuple2._1;
//            }
//        });
//
//
//        //============================
//        return null;
//    }
//
//
//    /**
//     *PV to mysql
//     * @param modifyLogDStream
//     * @return
//     */
//    private static JavaPairDStream<String, String> calculatePVSta(JavaPairDStream<String, String> modifyLogDStream) {
//        JavaPairDStream<String, String> filterDStream = modifyLogDStream.filter(new Function<Tuple2<String, String>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<String, String> tuple2) throws Exception {
//                if (tuple2 != null) {
//                    return true;
//                }
//                return false;
//            }
//        });
//        JavaPairDStream<String, Integer> mapPairDStream = filterDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
//           private static final long serialVersionUID = 1L;
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
//                String log = tuple2._2;
//                String[] logSplited = log.split("\t");
//                long timestamp = getTime(logSplited[7],0);
//                String region = getCity(logSplited[21].split(":")[1]);
//                String key = timestamp + "%" + region;
//                return new Tuple2<String, Integer>(key, 1);
//
//            }
//        });
//        JavaPairDStream<String, String> maptopair = mapPairDStream.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
//           private static final long serialVersionUID = 1L;
//            @Override
//            public Tuple2<String, String> call(Tuple2<String, Integer> tuple) throws Exception {
//                String[] split = tuple._1.split("%");
//                String timestamp = split[0];
//                String region = split[1];
//                return new Tuple2<String, String>(timestamp, region + "&" + tuple._2);
//            }
//        }).reduceByKey(new Function2<String, String, String>() {
//            @Override
//            public String call(String s, String s2) throws Exception {
//                return s + "#" + s2;
//            }
//        });
//       maptopair.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
//           private static final long serialVersionUID = 1L;
//            @Override
//            public Void call(JavaPairRDD<String, String> rdd) throws Exception {
//                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
//                    @Override
//                    public void call(Iterator<Tuple2<String, String>> tuples) throws Exception {
//
//                        List<Map<String,String>> pvStats = new ArrayList<Map<String,String>>();
//                        Tuple2<String, String> tuple = null;
//                        while (tuples.hasNext()) {
//                            Map<String, String> pvStat = new HashMap<String, String>();
//                            tuple = tuples.next();
//                            String[] region_count = tuple._2.split("#");
//                            pvStat.put("timestamp", tuple._1);
//                            for (String str : region_count) {
//                                String[] s = str.split("&");
//                                if (pvStat.get(s[0])!=null){
//                                    int num = Integer.parseInt(pvStat.get(s[0])) + Integer.parseInt(s[1]);
//                                    pvStat.put(s[0],num+"");
//                                }else {
//                                    pvStat.put(s[0], s[1]);
//                                }
//                            }
//                            pvStats.add(pvStat);
//                        }
//                        if (pvStats.size()>0){
//                            JDBCUtils jdbcUtils = JDBCUtils.getInstance();
//                            Connection conn = jdbcUtils.getConnection();
//                            ILogStatDAO logStatDAO = DAOFactory.getLogStatDAO();
//                            logStatDAO.updataBatch(pvStats,conn);
//                            System.out.println("mysql ==> "+pvStats.size());
//                            pvStats.clear();
//                            if (conn != null) {
//                                jdbcUtils.closeConnection(conn);
//                            }
//                        }
//
//                    }
//                });
//                return null;
//            }
//       });
//        return maptopair ;
//    }
//    /**
//     * 增加新的字段 地址加3ID \t 拼接原始数据
//     * @param filterLogDStream
//     * @return
//     */
//    private static JavaPairDStream<String,String> modifyByIPAndToken(JavaPairDStream<String, String> filterLogDStream) {
//        JavaPairDStream<String, String> mfJavaPairDStream = filterLogDStream.mapToPair(
//                new PairFunction<Tuple2<String, String>, String, String>() {
//                    @Override
//                    public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
//                        String token = "";
//                        String[] lines = tuple._2.split("\t");
//                        String mquID = "member_id:empty\tqz_id:empty\tuser_id:empty";
//                        String remote_addr = "country:empty\tregion:empty\tcity:empty";
//                        //求ip
//                        String ip = regexs(lines[0],0);
//                        if (!"".equals(ip)){
//                            String result = "";
//                            try {
//                               result = HttpReqUtil.getResult("ip/query",ip);
//                                try {
//                                   remote_addr = JSONUtil.getIPStr(result);
//                                }catch (Exception e){
//                                    System.out.println(result+" ==> 解析ip→json错误");
//                                    System.out.println(ip+" ==> json→ip");
//                                }
//                            }catch (Exception e){
//                                System.out.println(ip+" ==> read time out! 数据跳过");
//                            }
//                        }
//                        //esn or *api
//                        if ("esn".equals(lines[3])) {
//                            token = regexs(lines[19],1);
//                            if (!"".equals(token)){
//                                String result = "";
//                                try {
//                                    result = HttpReqUtil.getResult("user/redis/esn/" + token.split("=")[1], "");
//                                    mquID = JSONUtil.getmquStr(result);}
//                                catch (Exception e ){
//                                   System.out.println( token+"read time out! token数据跳过");
//                                   System.out.println(result + " ==> json→token");
//                                }
//                            }
//                        } else {
//                            if ("api".equals(lines[3])||"h5-api".equals(lines[3])||"pc-api".equals(lines[3])||"m".equals(lines[3])) {
//                                token = regexs(lines[10].replace("\\x22", "\"").replace("\"","").replace("\\x5C", ""),3);
//                                if ("".equals(token)) {
//                                    token = regexs(lines[9],2);
//                                }
//                            }
//                            if (!"".equals(token)){
//                                if (token.contains("=")){
//                                    token=token.split("=")[1];
//                                }else {
//                                    token = token.split(":")[1];
//                                }
//                                String result = "";
//                                try {
//                                    result = HttpReqUtil.getResult("user/redis/api/" + token, "");
//                                    mquID = JSONUtil.getmquStr(result);
//                                }catch (Exception e){
//                                    System.out.println(token.split("=")[1]+"read time out! token数据跳过");
//                                    System.out.println(result + " ==> json→token");
//                                }
//                            }
//                        }
//                        if (!remote_addr.contains("局域网")){
//                             String value = tuple._2 + "\t" + remote_addr + "\t" + mquID;
//                        System.out.println(lines[2] + "_" + lines[3] + "_" + lines[8] + "&&" + token + "==>" + lines[9]);
//                        return new Tuple2<String, String>(lines[2] + "_" + lines[3] + "_" + lines[8], value);
//                        }else {
//                            return null;
//                        }
//                    }
//                }
//        ).filter(new Function<Tuple2<String, String>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<String, String> tuple2) throws Exception {
//                if (tuple2 != null) {
//                    return true;
//                }
//                return false;
//            }
//        });
//        return mfJavaPairDStream;
//    }
//
//    /**
//     * 匹配ip+token
//     * @param line
//     * @param num
//     * @return
//     */
//    private static String regexs(String line,int num) {
//        //num 0 ip; 1 esn; 2 api
//        boolean rs = false;
//        String regEx = "";
//        String result = "";
//        switch (num) {
//            case 0://ip
//                regEx = "\\b(([01]?\\d?\\d|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d?\\d|2[0-4]\\d|25[0-5])\\b";
//                break;
//            case 1://PHPSESSID
//                regEx = "PHPSESSID=(?=.*[a-zA-Z])(?=.*[0-9])[a-zA-Z0-9]{26,26}";
//                break;
//            case 2://token
//                regEx = "token=(?=.*[a-zA-Z])(?=.*[0-9])[a-zA-Z0-9]{40,40}";
//                break;
//            case 3://token
//                regEx = "token:(?=.*[a-zA-Z])(?=.*[0-9])[a-zA-Z0-9]{40,40}";
//                break;
//        }
//        Pattern pat = Pattern.compile(regEx);
//        Matcher mat = pat.matcher(line);
//        rs = mat.find();
//        if (rs){
//            result = mat.group(0);
//        }
//
//        return result;
//    }
//
//    /**
//     * 获取请求中的token
//     * @param lines
//     * @return
//     */
//    private static String Token(String[] lines) {
//        String _token = "";
//        String s1 = lines[10].replace("\\x22", "\"").replace("\"","").replace("\\x5C", "");
//        String[] split = s1.split(",");
//        for (int i =0 ;i<split.length;i++){
//            if(split[i].contains("token")){
//                if (split[i].split(":").length==2)
//                    _token = split[i].split(":")[1].replace("}","").replace("{","");
//                break;
//            }
//        }
//        return _token;
//    }
//
//    /**
//     * filterData
//     * @param logDStream
//     * @return
//     */
//    private static JavaPairDStream<String,String> filterByRequest(JavaPairInputDStream<String, String> logDStream) {
//        return logDStream.filter(
//                new Function<Tuple2<String, String>, Boolean>() {
//                    @Override
//                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
//                        String[] log = tuple._2.split("\t");
//                        boolean flag  = false;
//                        if (log.length == 20 && "esn".equals(log[3])){
//                            flag = log[19].contains("PHPSESSID");
//                        }else if(log.length == 20 && ("api".equals(log[3]) || "h5-api".equals(log[3]) || "pc-api".equals(log[3]) || "m".equals(log[3]))){
//                            flag = tuple._2.contains("token");
//                        }
//                        return (tuple._2.contains(".htm") || ((!(tuple._2.contains(".js ")||tuple._2.contains(".css")||tuple._2.contains(".js?")||tuple._2.contains(".png")||tuple._2.contains(".gif"))) && flag && 1==1));
//                    }
//                }
//        );
//    }
//    private static String getCity(String city){
//        String str = "haiwai";
//        if ("澳门".equals(city)) {
//            str = "aomen";
//        }else if ("香港".equals(city)) {
//            str = "xianggang";
//        }else if ("台湾".equals(city)) {
//            str = "taiwan";
//        }else if ("广东".equals(city)) {
//            str = "guangdong";
//        }else if ("广西".equals(city)) {
//            str = "guangxi";
//        }else if ("海南".equals(city)) {
//            str = "hainan";
//        }else if ("云南".equals(city)) {
//            str = "yunnan";
//        }else if ("福建".equals(city)) {
//            str = "fujian";
//        }else if ("江西".equals(city)) {
//            str = "jiangxi";
//        }else if ("湖南".equals(city)) {
//            str = "hunan";
//        }else if ("贵州".equals(city)) {
//            str = "guizhou";
//        }else if ("浙江".equals(city)) {
//            str = "zhejiang";
//        }else if ("安徽".equals(city)) {
//            str = "anhui";
//        }else if ("上海".equals(city)) {
//            str = "shanghai";
//        }else if ("江苏".equals(city)) {
//            str = "jiangsu";
//        }else if ("湖北".equals(city)) {
//            str = "hubei";
//        }else if ("西藏".equals(city)) {
//            str = "xizang";
//        }else if ("青海".equals(city)) {
//            str = "qinghai";
//        }else if ("陕西".equals(city)) {
//            str = "shanxi_shan";
//        }else if ("山西".equals(city)) {
//            str = "shanxi_jin";
//        }else if ("河南".equals(city)) {
//            str = "henan";
//        }else if ("山东".equals(city)) {
//            str = "shandong";
//        }else if ("河北".equals(city)) {
//            str = "hebei";
//        }else if ("天津".equals(city)) {
//            str = "tianjin";
//        }else if ("北京".equals(city)) {
//            str = "beijing";
//        }else if ("宁夏".equals(city)) {
//            str = "ningxia";
//        }else if ("内蒙古".equals(city)) {
//            str = "neimeng";
//        }else if ("辽宁".equals(city)) {
//            str = "liaoning";
//        }else if ("吉林".equals(city)) {
//            str = "jilin";
//        }else if ("黑龙江".equals(city)) {
//            str = "heilongjiang";
//        }else if ("重庆".equals(city)) {
//            str = "chongqing";
//        }else if ("西川".equals(city)) {
//            str = "sichuan";
//        }else if ("局域网".equals(city)) {
//            str = "local";
//        }
//        return str;
//    }
//     private static long getTime(String timestamp , int num ) {
//        String strDateTime = timestamp.replace("[", "").replace("]", "");
//        long datekey = 0l;
//        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH);
//        SimpleDateFormat day = new SimpleDateFormat("yyyy-MM-dd");
//        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd HH");
//        Date t = null;
//        String format = "";
//        try {
//            t = formatter.parse(strDateTime);
//            if (1 == num ){
//                format = day.format(t);
//                t = day.parse(format);
//            } else if (2 == num) {
//                format = hour.format(t);
//                t = hour.parse(format);
//            }
//            datekey = t.getTime() / 1000;
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return datekey;
//    }
//}
//        //计算uvstat - hour
//        updatePairDStream.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
//                return new Tuple2<String, Integer>(tuple2._1.split("&")[0], 1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        }).foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
//            @Override
//            public Void call(JavaPairRDD<String, Integer> rdd) throws Exception {
//                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
//                    @Override
//                    public void call(Iterator<Tuple2<String, Integer>> tuple2) throws Exception {
//                        List<UVStat> uvStats = new ArrayList<UVStat>();
//                        Tuple2<String, Integer> tuple = null;
//                        while (tuple2.hasNext()) {
//                            tuple = tuple2.next();
//                            String type = "1hour";
//                            String clientType = "all";
//                            String created = tuple._1;
//                            Integer num = tuple._2;
//                            UVStat uvStat = new UVStat();
//                            uvStat.setType(type);
//                            uvStat.setClientType(clientType);
//                            uvStat.setCreated(created);
//                            uvStat.setNum(num);
//                            uvStats.add(uvStat);
//                        }
//                        if (uvStats.size()>0){
//                            JDBCUtils jdbcUtils = JDBCUtils.getInstance();
//                            Connection conn = jdbcUtils.getConnection();
//                            IUVStatDAO uvStatDAO = DAOFactory.getUVStatDAO();
//                            uvStatDAO.updataBatch(uvStats,conn);
//                            System.out.println("mysql to uvstat - hour ==> "+uvStats.size());
//                            uvStats.clear();
//                            if (conn != null) {
//                                jdbcUtils.closeConnection(conn);
//                            }
//                        }
//                    }
//                });
//                return null;
//            }
//        });
//        //计算uvstat - day
//        JavaPairDStream<String, Integer> windowToDay = updatePairDStream.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
//                String time = tuple2._1.split("&")[0];
//                long hour = Long.parseLong(time);
//                long dayTime = getDayLong(hour);
//                return new Tuple2<String, Integer>(dayTime + "", 1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//        windowToDay.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
//            @Override
//            public Void call(JavaPairRDD<String, Integer> rdd) throws Exception {
//                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
//                    @Override
//                    public void call(Iterator<Tuple2<String, Integer>> tuple2) throws Exception {
//                        List<UVStat> uvStats = new ArrayList<UVStat>();
//                        Tuple2<String, Integer> tuple = null;
//                        while (tuple2.hasNext()) {
//                            tuple = tuple2.next();
//                            String type = "1day";
//                            String clientType = "all";
//                            String created = tuple._1;
//                            Integer num = tuple._2;
//                            UVStat uvStat = new UVStat();
//                            uvStat.setType(type);
//                            uvStat.setClientType(clientType);
//                            uvStat.setCreated(created);
//                            uvStat.setNum(num);
//                            uvStats.add(uvStat);
//                        }
//                        if (uvStats.size()>0){
//                            JDBCUtils jdbcUtils = JDBCUtils.getInstance();
//                            Connection conn = jdbcUtils.getConnection();
//                            IUVStatDAO uvStatDAO = DAOFactory.getUVStatDAO();
//                            uvStatDAO.updataBatch(uvStats,conn);
//                            System.out.println("mysql to uvstat - day ==> "+uvStats.size());
//                            uvStats.clear();
//                            if (conn != null) {
//                                jdbcUtils.closeConnection(conn);
//                            }
//                        }
//                    }
//                });
//                return null;
//            }
//        });
