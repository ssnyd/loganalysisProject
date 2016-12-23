//package com.yonyou.myTest;
//
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//
///**
// * Created by chenxiaolei on 16/12/13.
// */
//public class Hello {
//    public static void main(String[] args) {
//        int i =-3;
//        System.out.println(i+"");
//        String s = i+"";
//        if (s.contains("-")){
//            System.out.println(s+"去你妹的");
//            System.out.println(s+"去你妹的");
//        }
//        //Object o = 1;
//        //int a =0;
//        //if (o instanceof Integer){
//        //    a = (Integer)o;
//        //}
//        //System.out.println(a);
//        SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd");
//        //获取前月的第一天
//        Calendar cal_1 = Calendar.getInstance();//获取当前日期
//        cal_1.add(Calendar.MONTH, -1);
//        cal_1.set(Calendar.DAY_OF_MONTH, 1);//设置为1号,当前日期既为本月第一天
//        String firstDay = format.format(cal_1.getTime());
//        System.out.println("-----1------firstDay:" + firstDay);
//        //获取前月的最后一天
//        Calendar cale = Calendar.getInstance();
//        cale.set(Calendar.DAY_OF_MONTH, 0);//设置为1号,当前日期既为本月第一天
//        String lastDay = format.format(cale.getTime());
//        System.out.println("-----2------lastDay:" + lastDay);
//        //获取当前月第一天：
//        Calendar c = Calendar.getInstance();
//        c.add(Calendar.MONTH, 0);
//        c.set(Calendar.DAY_OF_MONTH, 1);//设置为1号,当前日期既为本月第一天
//        String first = format.format(c.getTime());
//        System.out.println("===============first:" + first);
//        //获取当前月最后一天
//        Calendar ca = Calendar.getInstance();
//        ca.set(Calendar.DAY_OF_MONTH, ca.getActualMaximum(Calendar.DAY_OF_MONTH));
//        String last = format.format(ca.getTime());
//        System.out.println("===============last:" + last);
//        //获取当前周
//        Calendar cal = Calendar.getInstance();
//        cal.setFirstDayOfWeek(Calendar.MONDAY);//将每周第一天设为星期一，默认是星期天
//        cal.add(Calendar.DATE, 0);
//        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
//        String monday = format.format(cal.getTime());
//        System.out.println(monday);
//        //long time = DateUtils2.getTime("[11/Dec/2016:13:57:52 +0800]", 1);
//        //System.out.println(time);
//        //int actualMinimum = Calendar.getInstance().getActualMinimum(Calendar.DAY_OF_MONTH);
//        //System.out.println(actualMinimum);
//        //System.out.println(new Date());
//        //String todayTime = DateUtils.getTodayTime();
//        //  //System.out.println(todayTime);
//        //  SparkConf sconf = new SparkConf()
//        //          .setAppName("uvipvSpark")
//        //          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        //sconf.setMaster("local[2]");
//        //final int i =99;
//        //  JavaSparkContext sc = new JavaSparkContext(sconf);
//        //  JavaRDD<String> rdd = sc.textFile("file:////Users/chenxiaolei/rank.txt");
//        //  rdd.map(new Function<String, String>() {
//        //      @Override
//        //      public String call(String s) throws Exception {
//        //          return s+i;
//        //      }
//        //  }).foreach(new VoidFunction<String>() {
//        //      @Override
//        //      public void call(String s) throws Exception {
//        //          System.out.println(s);
//        //      }
//        //  });
//    }
//}
