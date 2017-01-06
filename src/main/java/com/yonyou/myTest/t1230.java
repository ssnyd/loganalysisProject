package com.yonyou.myTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by chenxiaolei on 16/12/30.
 */
public class t1230 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("ESNStreamingProject")
                .setMaster("local[2]");
        try {
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sc.textFile("file:////Users/chenxiaolei/t105.txt");
            long count = rdd.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String v1) throws Exception {
                    return v1.contains("&&");
                }
            }).map(new Function<String, String>() {

                @Override
                public String call(String v1) throws Exception {
                    return v1.split("&&")[0];
                }
            }).mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<String, Integer>(s, 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
                }
            }).sortByKey(false).filter(new Function<Tuple2<Integer, String>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Integer, String> v1) throws Exception {
                    return v1._1 > 1;
                }
            }).count();
            System.out.println(count);

        }catch (Exception e){
            System.out.println("hahahahahahahahaahahahah");
        }

////设置批次时间 10s
//        ddJavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));
//        //获取一个组
//        GroupCacheFactory factory = new GroupCacheFactory();
//        Group area = factory.group("area");
//        Group mqui = factory.group("mqui");
//        final Broadcast<Group> areaBro = jssc.sparkContext().broadcast(area);
//        final Broadcast<Group> mquiBro = jssc.sparkContext().broadcast(mqui);
//        JavaDStream<String> ds = jssc.textFileStream("file:////Users/chenxiaolei/stdout");
//        ds.map(new Function<String, String>() {
//            @Override
//            public String call(String v1) throws Exception {
//                Group value = areaBro.value();
//                if (value.getValue(v1)!=null){
//                    return v1+"!!!!ok";
//                }
//                value.push(v1,23,10);
//                return v1+"!!!!no";
//            }
//        }).foreachRDD(new VoidFunction<JavaRDD<String>>() {
//            @Override
//            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
//                System.out.println(stringJavaRDD);
//            }
//        });
    }
}
