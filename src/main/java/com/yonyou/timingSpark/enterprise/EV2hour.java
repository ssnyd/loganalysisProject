package com.yonyou.timingSpark.enterprise;

import com.yonyou.dao.IEVStatDAO;
import com.yonyou.dao.factory.DAOFactory;
import com.yonyou.entity.enterprise.EUPV;
import com.yonyou.entity.enterprise.EVStat;
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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by chenxiaolei on 16/12/13.
 */
public class EV2hour {
    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("ev2hour")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//      sconf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("accesslog"));
        scan.addColumn(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
        //scan.setStartRow(Bytes.toBytes(DateUtils.getYesterdayDate() + ":#"));
        //scan.setStopRow(Bytes.toBytes(DateUtils.getYesterdayDate() + "::"));
        scan.setStartRow(Bytes.toBytes(DateUtils.getlasthourDate() + ":#"));
        scan.setStopRow(Bytes.toBytes(DateUtils.getlasthourDate() + "::"));

        try {
            String tableName = "esn_accesslog";
            conf.set(TableInputFormat.INPUT_TABLE, tableName);
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD =
                    sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                            ImmutableBytesWritable.class, Result.class);
            //读取的每一行数据
            JavaRDD<String> filter = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                @Override
                public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {

                    byte[] value = v1._2.getValue(Bytes.toBytes("accesslog"), Bytes.toBytes("info"));
                    if (value != null) {
                        return Bytes.toString(value);
                    }
                    return null;
                }
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String v1) throws Exception {
                    return v1 != null;
                }
            }).repartition(100);
            //二次过滤 去除 企业ID不存在的
            JavaRDD<String> filter2emppry = filter2empty(filter);
            //格式转换
            JavaRDD<String> map2line = map2line(filter2emppry);
            map2line = map2line.persist(StorageLevel.MEMORY_AND_DISK_SER());
            //计算EV
            calculateEVSta(map2line);
            //计算EUV
            calculateEUVSta(map2line);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //计算EUV  时间戳 企业ID memID
    private static void calculateEUVSta(JavaRDD<String> map2line) {
        map2line.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] strings = s.split("&");
                return new Tuple2<String, String>(strings[0]+"&"+strings[1],strings[2]);
            }
        })
                .distinct()
                .mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, String> tuple2) throws Exception {
                        return new Tuple2<String, Integer>(tuple2._1,1);
                    }
                })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                List<EUPV> eupvStats = new ArrayList<EUPV>();
                while (iterator.hasNext()) {
                    EUPV eupvStat = new EUPV();
                    Tuple2<String, Integer> tuple2 = iterator.next();
                    eupvStat.setType("hour");
                    eupvStat.setCreated(tuple2._1.split("&")[0]);
                    eupvStat.setInstanceId(tuple2._1.split("&")[1]);
                    eupvStat.setEuvNum(tuple2._2);
                    eupvStats.add(eupvStat);
                }
                if (eupvStats.size() > 0) {
                    JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                    Connection conn = jdbcUtils.getConnection();

                    System.out.println("mysql 2 uvstat hour==> " + eupvStats.size());
                    eupvStats.clear();
                    if (conn != null) {
                        jdbcUtils.closeConnection(conn);
                    }
                }
            }
        });


    }

    //计算ev
    private static void calculateEVSta(JavaRDD<String> map2line) {
        JavaPairRDD<String, Integer> map = map2line.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Iterator<String> iterator) throws Exception {
                List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                while (iterator.hasNext()) {
                    String s = iterator.next();
                    String[] strings = s.split("&");
                    list.add(new Tuple2<String, String>(strings[0], strings[1]));
                }
                return list;
            }
        })
                .distinct()
                .mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, Integer>() {
                    @Override
                    public Iterable<Tuple2<String, Integer>> call(Iterator<Tuple2<String, String>> iterator2) throws Exception {
                        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
                        while (iterator2.hasNext()) {
                            Tuple2<String, String> tuple2 = iterator2.next();
                            list.add(new Tuple2<String, Integer>(tuple2._1, 1));
                        }

                        return list;
                    }
                });
        JavaPairRDD<String, Integer> rdd = map.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, 100);
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                List<EVStat> evStats = new ArrayList<EVStat>();
                while (tuple2Iterator.hasNext()) {
                    EVStat evStat = new EVStat();
                    Tuple2<String, Integer> tuple2 = tuple2Iterator.next();
                    evStat.setType("hour");
                    evStat.setCreated(tuple2._1);
                    evStat.setNum(tuple2._2);
                    evStats.add(evStat);
                }
                if (evStats.size() > 0) {
                    JDBCUtils jdbcUtils = JDBCUtils.getInstance();
                    Connection conn = jdbcUtils.getConnection();
                    IEVStatDAO evStatDAO = DAOFactory.getEVStatDAO();
                    evStatDAO.updataBatch(evStats, conn);
                    System.out.println("mysql 2 uvstat hour==> " + evStats.size());
                    evStats.clear();
                    if (conn != null) {
                        jdbcUtils.closeConnection(conn);
                    }
                }


            }
        });
    }

    //转换格式
    private static JavaRDD<String> map2line(final JavaRDD<String> line) {
        JavaRDD<String> stringJavaRDD = line.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterable<String> call(Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while (iterator.hasNext()) {
                    String s = iterator.next();
                    String[] str = s.split("\t");
                    long timestamp = DateUtils2.getTime(str[7], 2);
                    String instance_id = str[26].split(":")[1];//获得memid
                    String mem_id = str[23].split(":")[1];//获得memid
                    StringBuffer map = new StringBuffer();
                    //-------------------------
                    map.append(timestamp + "").append("&");
                    map.append(instance_id + "").append("&");
                    map.append(mem_id + "");
                    list.add(map.toString());
                }
                return list;
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.split("&").length == 3;
            }
        });
        return stringJavaRDD;
    }

    //过滤数据
    private static JavaRDD<String> filter2empty(JavaRDD<String> filter) {
        filter.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] str = s.split("\t");
                return str.length == 27 && str[23].split(":").length == 2 && str[26].split(":").length == 2;
            }
        });


        return null;
    }
}
