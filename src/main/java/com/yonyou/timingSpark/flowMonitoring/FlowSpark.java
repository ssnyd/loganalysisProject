package com.yonyou.timingSpark.flowMonitoring;

import com.yonyou.dao.FlowStatDAO;
import com.yonyou.dao.factory.DAOFactory;
import com.yonyou.entity.FlowStat;
import com.yonyou.jdbc.JDBCUtils;
import com.yonyou.utils.DateUtils;
import com.yonyou.utils.DateUtils2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chenxiaolei on 16/12/29.
 * com.yonyou.timingSpark.flowMonitoring.FlowSpark
 * 计算流量 及每天的pv
 */
public class FlowSpark {
    public static void main(String[] args) {
        try {
            //初始化
            JavaSparkContext sc = initSpark();
            //获取元数据
            JavaRDD<String> source = resourceRDD(args, sc);
            //变化 time site ip mem tra
            JavaRDD<String> maprdd = mapRDD(source);
            //缓存到内存 复用rdd
            maprdd = maprdd.persist(StorageLevel.MEMORY_ONLY());
            //计算 流量RDD 并存储
            calculaTeaffic2Mysql(maprdd);
            //计算 uvRDD 并存储
            calculaUV2Mysql(maprdd);
            //计算 pvRDD 并存储
            calculaPV2Mysql(maprdd);
            //计算 ipvRDD 并存储
            calculaIPV2Mysql(maprdd);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 计算ipv 存储到mysql
     * @param maprdd
     */
    private static void calculaIPV2Mysql(JavaRDD<String> maprdd) {
        maprdd.map(new Function<String, String>() {
            private static final long serialVersionUID = 2692248415538872586L;

            @Override
            public String call(String v1) throws Exception {
                String[] lines = v1.split("&");
                return lines[0] + "&" + lines[1] + "&" + lines[2];
            }
        }).distinct().mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = -502098811580205184L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] lines = s.split("&");
                return new Tuple2<String, Integer>(lines[0] +"&"+ lines[1], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -7456140153435084455L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                List<FlowStat> flowStats = new ArrayList<FlowStat>();
                Tuple2<String, Integer> tuple = null;
                while (iterator.hasNext()) {
                    tuple = iterator.next();
                    String[] str = tuple._1.split("&");
                    int ipv = tuple._2;
                    FlowStat flowStat = new FlowStat();
                    flowStat.setCreated(str[0]);
                    flowStat.setFlow(ipv);
                    flowStat.setSiteType(str[1]);
                    flowStat.setType("1day");
                    flowStats.add(flowStat);
                }
                if (flowStats.size() > 0) {
                    JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                    Connection conn = jdbcUtils.getConnection();
                    FlowStatDAO flowStatDAO = DAOFactory.getFlowStatDAO();
                    flowStatDAO.updataBatch(flowStats, conn, 2);
                    System.out.println("mysql 2 ipv==> " + flowStats.size());
                    flowStats.clear();
                    if (conn != null) {
                        jdbcUtils.closeConnection(conn);
                    }
                }
            }
        });




    }

    /**
     *计算pv 存储到mysql
     * @param maprdd
     */
    private static void calculaPV2Mysql(JavaRDD<String> maprdd) {
        maprdd.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1421708056182878792L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] lines = s.split("&");
                return new Tuple2<String, Integer>(lines[0] + "&" + lines[1],1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -1422762837271470209L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
            private static final long serialVersionUID = 3812121298368817680L;

            @Override
            public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                List<FlowStat> flowStats = new ArrayList<FlowStat>();
                Tuple2<String, Integer> tuple = null;
                while (iterator.hasNext()) {
                    tuple = iterator.next();
                    String[] str = tuple._1.split("&");
                    int pv = tuple._2;
                    FlowStat flowStat = new FlowStat();
                    flowStat.setCreated(str[0]);
                    flowStat.setFlow(pv);
                    flowStat.setSiteType(str[1]);
                    flowStat.setType("1day");
                    flowStats.add(flowStat);
                }
                if (flowStats.size() > 0) {
                    JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                    Connection conn = jdbcUtils.getConnection();
                    FlowStatDAO flowStatDAO = DAOFactory.getFlowStatDAO();
                    flowStatDAO.updataBatch(flowStats, conn, 1);
                    System.out.println("mysql 2 pv==> " + flowStats.size());
                    flowStats.clear();
                    if (conn != null) {
                        jdbcUtils.closeConnection(conn);
                    }
                }
            }
        });
    }

    /**
     * 计算uv 存储到mysql
     *
     * @param maprdd
     */
    private static void calculaUV2Mysql(JavaRDD<String> maprdd) {
        maprdd.filter(new Function<String, Boolean>() {
            private static final long serialVersionUID = -3607766750437881657L;

            @Override
            public Boolean call(String v1) throws Exception {
                return !v1.contains("empty");
            }
        }).map(new Function<String, String>() {
            private static final long serialVersionUID = 4587494602573607903L;

            @Override
            public String call(String v1) throws Exception {
                String[] lines = v1.split("&");
                return lines[0] + "&" + lines[1] + "&" + lines[3];
            }
        }).distinct().mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 2694226103861018569L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] lines = s.split("&");
                return new Tuple2<String, Integer>(lines[0] +"&"+ lines[1], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                List<FlowStat> flowStats = new ArrayList<FlowStat>();
                Tuple2<String, Integer> tuple = null;
                while (iterator.hasNext()) {
                    tuple = iterator.next();
                    String[] str = tuple._1.split("&");
                    int uv = tuple._2;
                    FlowStat flowStat = new FlowStat();
                    flowStat.setCreated(str[0]);
                    flowStat.setFlow(uv);
                    flowStat.setSiteType(str[1]);
                    flowStat.setType("1day");
                    flowStats.add(flowStat);
                }
                if (flowStats.size() > 0) {
                    JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                    Connection conn = jdbcUtils.getConnection();
                    FlowStatDAO flowStatDAO = DAOFactory.getFlowStatDAO();
                    flowStatDAO.updataBatch(flowStats, conn, 0);
                    System.out.println("mysql 2 uv==> " + flowStats.size());
                    flowStats.clear();
                    if (conn != null) {
                        jdbcUtils.closeConnection(conn);
                    }
                }
            }
        });


    }

    /**
     * 计算流量 存储到mysql
     *
     * @param maprdd
     */
    private static void calculaTeaffic2Mysql(JavaRDD<String> maprdd) {
        maprdd.mapToPair(new PairFunction<String, String, Long>() {
            private static final long serialVersionUID = 1421708056182878792L;

            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                String[] lines = s.split("&");
                return new Tuple2<String, Long>(lines[0] + "&" + lines[1], Long.parseLong(lines[4]));
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            private static final long serialVersionUID = -1422762837271470209L;

            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, 1).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
            private static final long serialVersionUID = 3812121298368817680L;

            @Override
            public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                List<FlowStat> flowStats = new ArrayList<FlowStat>();
                Tuple2<String, Long> tuple = null;

                while (iterator.hasNext()) {
                    tuple = iterator.next();
                    String[] str = tuple._1.split("&");
                    Long traffic = tuple._2;
                    FlowStat flowStat = new FlowStat();
                    flowStat.setCreated(str[0]);
                    flowStat.setFlow(traffic);
                    flowStat.setSiteType(str[1]);
                    flowStat.setType("1day");
                    flowStats.add(flowStat);
                }
                if (flowStats.size() > 0) {
                    JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                    Connection conn = jdbcUtils.getConnection();
                    FlowStatDAO flowStatDAO = DAOFactory.getFlowStatDAO();
                    flowStatDAO.updataBatch(flowStats, conn, 3);
                    System.out.println("mysql 2 flowStats==> " + flowStats.size());
                    flowStats.clear();
                    if (conn != null) {
                        jdbcUtils.closeConnection(conn);
                    }
                }
            }
        });


    }

    /**
     * 复用rdd
     *
     * @param source maprdd
     * @return
     */
    private static JavaRDD<String> mapRDD(JavaRDD<String> source) {
        return source.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            private static final long serialVersionUID = 7108232044577750023L;

            @Override
            public Iterable<String> call(Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                String[] lines = null;
                long time = 0l;
                String siteType = null;
                String ip = null;
                String memberId = "";
                int traffic = 0;
                while (iterator.hasNext()) {
                    lines = iterator.next().split("\t");
                    time = DateUtils2.getTime(lines[7], 1);
                    siteType = lines[3];
                    ip = lines[0];
                    if (lines[23].split(":").length == 2) {
                        memberId = lines[23].split(":")[1];
                    }
                    traffic = getVal(lines[13]);
                    //time site ip mem tra
                    list.add(time + "&" + siteType + "&" + ip + "&" + memberId + "&" + traffic);
                }
                return list;
            }
        });
    }


    /**
     * @param args
     * @param sc
     * @return
     * @throws IOException
     */
    private static JavaRDD<String> resourceRDD(String[] args, JavaSparkContext sc) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("accesslog"));
        scan.addColumn(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
        if (args.length == 2) {
            scan.setStartRow(Bytes.toBytes(args[0] + ":#"));
            scan.setStopRow(Bytes.toBytes(args[1] + "::"));
        } else {
            scan.setStartRow(Bytes.toBytes(DateUtils.getYesterdayDate() + ":#"));
            scan.setStopRow(Bytes.toBytes(DateUtils.getYesterdayDate() + "::"));
        }

        final String tableName = "esn_accesslog";
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String ScanToString = Base64.encodeBytes(proto.toByteArray());
        conf.set(TableInputFormat.SCAN, ScanToString);
        JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class).repartition(150);
        //读取的每一行数据
        return myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
            @Override
            public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {

                byte[] value = v1._2.getValue(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
                if (value != null) {
                    return Bytes.toString(value);
                }
                return null;
            }
        });
    }

    /**
     * @param key
     * @return
     */
    private static int getVal(String key) {
        int res = 0;
        try {
            res = Integer.parseInt(key);
        } catch (Exception e) {
            System.out.println("存在非法字符 ==>" + key);
            return 0;
        }
        return res;
    }

    /*
     * 初始化spark
	 */
    private static JavaSparkContext initSpark() {

        SparkConf sparkConf = new SparkConf()
                .setAppName("FlowSpark")
                .set("spark.default.parallelism", "150")//並行度，reparation后生效(因为集群现在的配置是8核，按照每个核心有一个vcore，就是16，三个worker节点，就是16*3，并行度设置为3倍的话：16*3*3=144，故，这里设置150)
                .set("spark.locality.wait", "100ms")
                .set("spark.shuffle.manager", "hash")//使用hash的shufflemanager
                .set("spark.shuffle.consolidateFiles", "true")//shufflemap端开启合并较小落地文件（hashshufflemanager方式一个task对应一个文件，开启合并，reduce端有几个就是固定几个文件，提前分配好省着merge了）
                .set("spark.shuffle.file.buffer", "64")//shufflemap端mini环形缓冲区bucket的大小调大一倍，默认32KB
                .set("spark.reducer.maxSizeInFlight", "24")//从shufflemap端拉取数据24，默认48M
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//序列化
                .set("spark.shuffle.io.maxRetries", "10")//GC重试次数，默认3
                .set("spark.shuffle.io.retryWait", "30s");//GC等待时长，默认5s
//				.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        return jsc;

    }
}
