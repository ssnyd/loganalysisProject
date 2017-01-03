//package com.yonyou.myTest;
//
//import com.yonyou.cxl.cache.Group;
//import com.yonyou.cxl.cache.GroupCacheFactory;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.broadcast.Broadcast;
//
///**
// * Created by chenxiaolei on 16/12/30.
// */
//public class t1230 {
//    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf()
//                .setAppName("ESNStreamingProject")
//                .setMaster("local[4]");
//        try {
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        GroupCacheFactory factory = new GroupCacheFactory();
//        Group area = factory.group("area");
//        Group mqui = factory.group("mqui");
//        final Broadcast<Group> areaBro = sc.broadcast(area);
//        final Broadcast<Group> mquiBro = sc.broadcast(mqui);
//        JavaRDD<String> rdd = sc.textFile("file:////Users/chenxiaolei/stdout");
//        rdd.map(new Function<String, String>() {
//            @Override
//            public String call(String v1) throws Exception {
//                System.out.println("!!!!!!!!!!!!!!!!!");
//                    Group value = areaBro.value();
//                    System.out.println(value.getValue(v1)+Thread.currentThread().getName());
//                    if (value.getValue(v1)!=null){
//                        return v1+"!!!!ok"+value.getValue(v1);
//                    }
//                    value.push(v1,23,5);
//                    return v1+"!!!!no"+value.getValue(v1);
//            }
//        }).foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                Thread.sleep(1000);
//                System.out.println(s);
//            }
//        });
//        }catch (Exception e){
//            System.out.println("hahahahahahahahaahahahah");
//        }
//
//////设置批次时间 10s
////        ddJavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));
////        //获取一个组
////        GroupCacheFactory factory = new GroupCacheFactory();
////        Group area = factory.group("area");
////        Group mqui = factory.group("mqui");
////        final Broadcast<Group> areaBro = jssc.sparkContext().broadcast(area);
////        final Broadcast<Group> mquiBro = jssc.sparkContext().broadcast(mqui);
////        JavaDStream<String> ds = jssc.textFileStream("file:////Users/chenxiaolei/stdout");
////        ds.map(new Function<String, String>() {
////            @Override
////            public String call(String v1) throws Exception {
////                Group value = areaBro.value();
////                if (value.getValue(v1)!=null){
////                    return v1+"!!!!ok";
////                }
////                value.push(v1,23,10);
////                return v1+"!!!!no";
////            }
////        }).foreachRDD(new VoidFunction<JavaRDD<String>>() {
////            @Override
////            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
////                System.out.println(stringJavaRDD);
////            }
////        });
//    }
//}
