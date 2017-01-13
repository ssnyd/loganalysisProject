//package com.yonyou.timingSpark.temporaryTask;
//
//import com.yonyou.jdbc.JDBCHelper;
//import com.yonyou.jdbc.JDBCUtils;
//import com.yonyou.utils.DateUtils;
//import com.yonyou.utils.HttpReqUtil;
//import com.yonyou.utils.JSONUtil;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
//import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
//import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
//import org.apache.hadoop.hbase.util.Base64;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.storage.StorageLevel;
//import scala.Tuple2;
//
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.*;
//
///**
// * 2016年12月15～2017年1月2
// * rp_activity_data 数据表 中memberId
// * 查看statisticsRegister字段 distinct member
// * 根据 member集合筛选对应用户 求应用uv pv 按天分开
// * Created by chenxiaolei on 17/1/3.
// */
//public class BatchApplySpark {
//    public static void main(String[] args) {
//        // 构建Spark上下文
//        SparkConf sconf = new SparkConf()
//                .setAppName("batchApplySpark")
////				.set("spark.default.parallelism", "100")
//                .set("spark.storage.memoryFraction", "0.5")
//                .set("spark.shuffle.consolidateFiles", "true")
//                .set("spark.shuffle.file.buffer", "64")
//                .set("spark.shuffle.memoryFraction", "0.3")
//                .set("spark.reducer.maxSizeInFlight", "24")
//                .set("spark.shuffle.io.maxRetries", "60")
//                .set("spark.shuffle.io.retryWait", "60")
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        JavaSparkContext sc = new JavaSparkContext(sconf);
//        Configuration conf = HBaseConfiguration.create();
//        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("app_case"));
//        scan.addColumn(Bytes.toBytes("app_case"), Bytes.toBytes("log"));
////      scan.setStartRow(Bytes.toBytes(getTimes("2016:11:28")+":#"));
////      scan.setStopRow(Bytes.toBytes(getTimes("2016:11:28")+"::"));
//        if (args.length == 2) {
//            scan.setStartRow(Bytes.toBytes(getTimes(args[0]) + ":#"));
//            scan.setStopRow(Bytes.toBytes(getTimes(args[1]) + "::"));
//        } else {
//            scan.setStartRow(Bytes.toBytes(getTimes(DateUtils.getYesterdayDate()) + ":#"));
//            scan.setStopRow(Bytes.toBytes(getTimes(DateUtils.getYesterdayDate()) + "::"));
//        }
//        //获取mysql 白名单 广播出去
//        List<Tuple2<String, Boolean>> list = getWhiteList();
//        final Broadcast<List<Tuple2<String, Boolean>>> whiteListBroadcast = sc.broadcast(list);
//        try {
//            final String tableName = "esn_datacollection";
//            conf.set(TableInputFormat.INPUT_TABLE, tableName);
//            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
//            String ScanToString = Base64.encodeBytes(proto.toByteArray());
//            conf.set(TableInputFormat.SCAN, ScanToString);
//            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
//                    sc.newAPIHadoopRDD(conf, TableInputFormat.class,
//                            ImmutableBytesWritable.class, Result.class).repartition(100);
//            //读取的每一行数据
//            JavaRDD<String> filterRDD = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
//                @Override
//                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
//
//                    byte[] value = v1._2.getValue(Bytes.toBytes("app_case"), Bytes.toBytes("log"));
//                    if (value != null) {
//                        return Bytes.toString(value);
//                    }
//                    return null;
//                }
//            }).filter(new Function<String, Boolean>() {
//                @Override
//                public Boolean call(String v1) throws Exception {
//                    return v1 != null;
//                }
//            })
//                    //"action":"click" or "action":"_click" 只留下
//                    .filter(new Function<String, Boolean>() {
//                        @Override
//                        public Boolean call(String v1) throws Exception {
//                            return v1.contains("click");
//                        }
//                    }).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
//                        @Override
//                        public Iterable<String> call(Iterator<String> iterator) throws Exception {
//                            List<String> list = new ArrayList<>();
//                            String line = "";
//                            while (iterator.hasNext()) {
//                                //action:view&app_id:22239&instance_id:3219&qz_id:3968&member_id:3469&mtime:1480044831884
//                                line = JSONUtil.getappId(iterator.next());
//                                list.add(line);
//                            }
//                            return list;
//                        }
//                    });
//
//            //获得openid
//            JavaRDD<String> openIdRDD = filterRDD.filter(new Function<String, Boolean>() {
//                @Override
//                public Boolean call(String line) throws Exception {
//                    return line.split("&").length > 2 && line.split("&")[1].split(":").length == 2;
//                }
//            }).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
//                @Override
//                public Iterable<String> call(Iterator<String> iterator) throws Exception {
//                    List<String> list = new ArrayList<>();
//                    String app_id = "";
//                    String line = "";
//                    while (iterator.hasNext()) {
//                        line = iterator.next();
//                        app_id = line.split("&")[1].split(":")[1];
//                        if (app_id.contains("-") ||app_id.contains("+")) {
//                            String[] str = line.split("&");
//                            line = "open_appid:" + app_id + "&" + "name:empty" + "&" + str[0] + "&" + "app_id:0" + "&" + str[2] + "&" + str[3] + "&" + str[4] + "&" + str[5];
//                        } else {
//
//                            String opid = JSONUtil.getopenId(HttpReqUtil.getResult("app/info/" + app_id, ""));
//                            if (!"".equals(opid)){
//                                line = opid + "&" + line;
//
//                            }
//                        }
//                        //open_appid:110 &name:协同日程新 &action:view &app_id:22239 &instance_id:3219 &qz_id:3968 &member_id:3469 &mtime:1480044831884
//                        list.add(line);
//                    }
//                    return list;
//                }
//            }).filter(new Function<String, Boolean>() {
//                @Override
//                public Boolean call(String s) throws Exception {
//                    String[] split = s.split("&");
//
//                    return split[1].split(":").length == 2 && split[0].split(":").length == 2 && split.length==8;
//                }
//            }).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
//                @Override
//                public Iterable<String> call(Iterator<String> iterator) throws Exception {
//                    List list = new ArrayList();
//                    String app_id = null;
//                    String line = null;
//                    while (iterator.hasNext()) {
//                        line = iterator.next();
//                        String[] str = line.split("&");
//                        if (!"0".equals(str[0].split(":")[1])) {
//                            app_id = "app_id:0";
//                            line = str[0] + "&" + str[1] + "&" + str[2] + "&" + app_id + "&" + str[4] + "&" + str[5] + "&" + str[6] + "&" + str[7];
//                        }
//                        list.add(line);
//                    }
//                    return list;
//                }
//            });
//
//            //open_appid:110 &name:协同日程新 &action:view &app_id:22239 &instance_id:3219 &qz_id:3968 &member_id:3469 &mtime:1480044831884
//
//            JavaRDD<String> map = openIdRDD.map(new Function<String, String>() {
//                @Override
//                public String call(String v1) throws Exception {
//                    String[] str = v1.split("&");
//                    //name memeid mtime opappid
//                    return str[1].split(":")[1] + "&" + str[6].split(":")[1] + "&" + getDays(str[7].split(":")[1])+"&"+str[0].split(":")[1];
//                }
//            });
//
//            //过滤数据
//            JavaRDD<String> memberrdd = filterMemberId(map, whiteListBroadcast);
//            memberrdd = memberrdd.persist(StorageLevel.MEMORY_ONLY_SER());
//            //求pv
//            memberrdd.mapToPair(new PairFunction<String, String, Integer>() {
//                private static final long serialVersionUID = -2819615402806085813L;
//
//                @Override
//                public Tuple2<String, Integer> call(String s) throws Exception {
//                    //-----------------
//                    //name memeid mtime opappid
//                    String[] str = s.split("&");
//                    //mtime opid name ,1
//                    return new Tuple2<String, Integer>(str[2]+"&"+str[3]+"&"+str[0],1);
//                }
//            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//                @Override
//                public Integer call(Integer v1, Integer v2) throws Exception {
//                    return v1+v2;
//                }
//            },1).saveAsTextFile("hdfs://cluster/0103/pv");
//            //存uv
//            memberrdd.distinct().mapToPair(new PairFunction<String, String, Integer>() {
//                private static final long serialVersionUID = 5083326587920548591L;
//
//                @Override
//                public Tuple2<String, Integer> call(String s) throws Exception {
//                    String[] str = s.split("&");
//                    return new Tuple2<String, Integer>(str[2]+"&"+str[3]+"&"+str[0],1);
//                }
//            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//                @Override
//                public Integer call(Integer v1, Integer v2) throws Exception {
//                    return v1+v2;
//                }
//            },1).saveAsTextFile("hdfs://cluster/0103/uv");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    //获得白名单
//    private static List<Tuple2<String, Boolean>> getWhiteList() {
//        final List<Tuple2<String, Boolean>> list = new ArrayList<Tuple2<String, Boolean>>();
//        String selectSQL = "select distinct memberId from rp_activity_data where action = 'statisticsRegister' and created >=1481731200 and created <= 1483286400";
//        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
//        Connection conn = jdbcUtils.getConnection();
//        JDBCHelper.executeQuery(conn, selectSQL, null, new JDBCHelper.QueryCallback() {
//            @Override
//            public void process(ResultSet rs) throws Exception {
//                while (rs.next()) {
//                    list.add(new Tuple2<String, Boolean>(rs.getInt(1) + "", true));
//                }
//            }
//        });
//        return list;
//    }
//
//    //根据白名单 也就是mysql里面的数据 筛选数据
//    private static JavaRDD<String> filterMemberId(JavaRDD<String> filterRDD, final Broadcast<List<Tuple2<String, Boolean>>> whiteListBroadcast) {
//
//        JavaPairRDD<String, String> mapRDD = filterRDD.mapToPair(new PairFunction<String, String, String>() {
//            private static final long serialVersionUID = -407939968798540149L;
//
//            @Override
//            public Tuple2<String, String> call(String s) throws Exception {
//                //name memeid mtime opappid
//                String key = s.split("&")[1];
//                return new Tuple2<String, String>(key, s);
//            }
//        });
//
//        JavaPairRDD<String, String> whiterdd = mapRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, String>>, Boolean, Tuple2<String, String>>() {
//            private static final long serialVersionUID = 6136268113645750637L;
//
//            @Override
//            public Iterable<Tuple2<Boolean, Tuple2<String, String>>> call(Iterator<Tuple2<String, String>> iterator) throws Exception {
//                List<Tuple2<Boolean, Tuple2<String, String>>> list = new ArrayList<Tuple2<Boolean, Tuple2<String, String>>>();
//                List<Tuple2<String, Boolean>> value = whiteListBroadcast.value();
//                Map<String, Boolean> whiteMap = new HashMap<String, Boolean>();
//                for (Tuple2<String, Boolean> memberId : value) {
//                    whiteMap.put(memberId._1, memberId._2);
//                }
//                while (iterator.hasNext()) {
//                    Tuple2<String, String> tuple2 = iterator.next();
//                    Boolean flag = whiteMap.get(tuple2._1);
//                    if (flag == null) {
//                        list.add(new Tuple2<Boolean, Tuple2<String, String>>(false, tuple2));
//                    } else {
//                        list.add(new Tuple2<Boolean, Tuple2<String, String>>(true, tuple2));
//                    }
//
//                }
//                return list;
//            }
//        }).filter(new Function<Tuple2<Boolean, Tuple2<String, String>>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<Boolean, Tuple2<String, String>> v1) throws Exception {
//                return v1._1;
//            }
//        }).mapToPair(new PairFunction<Tuple2<Boolean, Tuple2<String, String>>, String, String>() {
//            private static final long serialVersionUID = -6376318580449619385L;
//
//            @Override
//            public Tuple2<String, String> call(Tuple2<Boolean, Tuple2<String, String>> tuple2) throws Exception {
//                return tuple2._2;
//            }
//        });
//        return whiterdd.map(new Function<Tuple2<String, String>, String>() {
//            @Override
//            public String call(Tuple2<String, String> tuple2) throws Exception {
//                return tuple2._2;
//            }
//        });
//    }
//
//    /**
//     * 获得当天时间戳 hbase rowkey
//     *
//     * @param date
//     * @return
//     */
//    private static long getTimes(String date) {
//        SimpleDateFormat day = new SimpleDateFormat("yyyy:MM:dd");
//        Date parse = null;
//        long l = 0l;
//        try {
//            parse = day.parse(date);
//            l = parse.getTime() / 1000;
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return l;
//    }
//
//    private static long getDays(String times) {
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//        Date date = null;
//        try {
//            Long time = Long.parseLong(times);
//            String d = format.format(time);
//            date = format.parse(d);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return date.getTime() / 1000;
//    }
//}
