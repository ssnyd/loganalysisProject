package com.yonyou.myTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Created by chenxiaolei on 16/12/13.
 */
public class Hello {
    public static void main(String[] args) {
        //System.out.println(new Date());
        //String todayTime = DateUtils.getTodayTime();
        //System.out.println(todayTime);
        SparkConf sconf = new SparkConf()
                .setAppName("uvipvSpark")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
      sconf.setMaster("local[2]");
      final int i =99;
        JavaSparkContext sc = new JavaSparkContext(sconf);
        JavaRDD<String> rdd = sc.textFile("file:////Users/chenxiaolei/rank.txt");
        rdd.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s+i;
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


    }
}
