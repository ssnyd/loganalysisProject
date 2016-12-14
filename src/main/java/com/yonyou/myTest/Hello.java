package com.yonyou.myTest;

import com.yonyou.utils.DateUtils2;

/**
 * Created by chenxiaolei on 16/12/13.
 */
public class Hello {
    public static void main(String[] args) {
        long time = DateUtils2.getTime("[11/Dec/2016:13:57:52 +0800]", 1);
        System.out.println(time);

        //System.out.println(new Date());
        //String todayTime = DateUtils.getTodayTime();
      //  //System.out.println(todayTime);
      //  SparkConf sconf = new SparkConf()
      //          .setAppName("uvipvSpark")
      //          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
      //sconf.setMaster("local[2]");
      //final int i =99;
      //  JavaSparkContext sc = new JavaSparkContext(sconf);
      //  JavaRDD<String> rdd = sc.textFile("file:////Users/chenxiaolei/rank.txt");
      //  rdd.map(new Function<String, String>() {
      //      @Override
      //      public String call(String s) throws Exception {
      //          return s+i;
      //      }
      //  }).foreach(new VoidFunction<String>() {
      //      @Override
      //      public void call(String s) throws Exception {
      //          System.out.println(s);
      //      }
      //  });


    }
}
