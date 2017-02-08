package com.yonyou.myTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by chenxiaolei on 17/1/22.
 */
public class t122 {
    public static void main(String[] args) {
        SparkConf sconf = new SparkConf()
                .setAppName("EUUV2day")
                .set("spark.default.parallelism", "3")//並行度，reparation后生效(因为集群现在的配置是8核，按照每个核心有一个vcore，就是16，三个worker节点，就是16*3，并行度设置为3倍的话：16*3*3=144，故，这里设置150)
                .set("spark.locality.wait", "100ms")
                .set("spark.shuffle.manager", "hash")//使用hash的shufflemanager
                .set("spark.shuffle.consolidateFiles", "true")//shufflemap端开启合并较小落地文件（hashshufflemanager方式一个task对应一个文件，开启合并，reduce端有几个就是固定几个文件，提前分配好省着merge了）
                .set("spark.shuffle.file.buffer", "64")//shufflemap端mini环形缓冲区bucket的大小调大一倍，默认32KB
                .set("spark.reducer.maxSizeInFlight", "24")//从shufflemap端拉取数据24，默认48M
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//序列化
                .set("spark.shuffle.io.maxRetries", "10")//GC重试次数，默认3
                .set("spark.shuffle.io.retryWait", "30s");//GC等待时长，默认5s
        sconf.setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sconf);
        List<String> tuples = new ArrayList<String>();
        tuples.add("chen");
        tuples.add("chen1");
        tuples.add("chen2");
        tuples.add("chen3");
        Map<String,Boolean> map = new HashMap<String,Boolean>();
        for (String s :tuples){
            map.put(s,true);
        }
        final Broadcast<Map<String, Boolean>> broadcast = sc.broadcast(map);

        final List<Tuple2<String, String>> tuple = new ArrayList<Tuple2<String, String>>();
        tuple.add(new Tuple2<String, String>("chen", "chen001001a"));
        tuple.add(new Tuple2<String, String>("chen1", "chen001001b"));
        tuple.add(new Tuple2<String, String>("chen2", "chen001001c"));
        tuple.add(new Tuple2<String, String>("chen3", "chen001001d"));
        tuple.add(new Tuple2<String, String>("chen4", "chen001001e"));
        tuple.add(new Tuple2<String, String>("chen5", "chen001001f"));
        tuple.add(new Tuple2<String, String>("chen1", "chen001001g"));
        tuple.add(new Tuple2<String, String>("chen2", "chen001001h"));
        tuple.add(new Tuple2<String, String>("chen1", "chen001001j"));
        tuple.add(new Tuple2<String, String>("chen3", "chen001001k"));
        JavaPairRDD<String, String> t1 = sc.parallelizePairs(tuple);
        t1.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                Map<String, Boolean> value = broadcast.value();
                if (value.get(v1._1) == null){
                    return false;
                }
               return true;
            }
        }).foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
    }
}
