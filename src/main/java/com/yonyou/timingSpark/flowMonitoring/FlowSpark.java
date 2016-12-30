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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chenxiaolei on 16/12/29.
 */
public class FlowSpark {
    public static void main(String[] args) {
        try {
            //初始化
            JavaSparkContext sc = initSpark();
            //获取元数据
            JavaRDD<String> filter = resourceRDD(args, sc);
            //获取流量RDD
            JavaPairRDD<String, Double> flowRDD = getFlowRDD(filter);
            //存结果mysql
            flow2mysql(flowRDD);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param flowRDD
     */
    private static void flow2mysql(JavaPairRDD<String, Double> flowRDD) {
        flowRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Double>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Double>> iterator) throws Exception {
                List<FlowStat> flowStats = new ArrayList<FlowStat>();
                Tuple2<String, Double> tuple = null;
                while (iterator.hasNext()) {
                    tuple = iterator.next();
                    String[] str = tuple._1.split("&");
                    String created = str[0];
                    String siteType = str[1];
                    Double flow = tuple._2;
                    FlowStat flowStat = new FlowStat();
                    flowStat.setCreated(created);
                    flowStat.setFlow(flow);
                    flowStat.setSiteType(siteType);
                    flowStat.setType("1hour");
                    flowStats.add(flowStat);
                }
                if (flowStats.size() > 0) {
                    JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                    Connection conn = jdbcUtils.getConnection();
                    FlowStatDAO flowStatDAO = DAOFactory.getFlowStatDAO();
                    flowStatDAO.updataBatch(flowStats, conn);
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
     *
     * @param filter
     * @return
     */
    private static JavaPairRDD<String, Double> getFlowRDD(JavaRDD<String> filter) {
        //数据 求按来源分类 每小时的流量 比如 esn api 之类的 开始转换
        return filter.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                String[] keys = s.split("\t");
                long time = DateUtils2.getTime(keys[7], 2);
                String type = keys[3];
                double value = getVal(keys[13]);
                return new Tuple2<String, Double>(time + "&" + type, value);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        }, 1);
    }

    /**
     *
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
     *
     * @param key
     * @return
     */
    private static double getVal(String key) {
        double res = 0.0d;
        try {
            res = Double.parseDouble(key);
        } catch (Exception e) {
            System.out.println("存在非法字符 ==>" + key);
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
