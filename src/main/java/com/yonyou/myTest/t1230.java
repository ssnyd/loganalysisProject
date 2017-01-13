//package com.yonyou.myTest;
//
///**
// * Created by chenxiaolei on 16/12/30.
// */
//
//public class t1230 {
//    public static void main(String[] args) {
//        //Map<String, String> map = new HashMap<String, String>();
//        String i ="1.3";
//        System.out.println(Integer.parseInt(i));
//        //String ipi = "a";
//        //int i = 1;
//        //while (i < 1000) {
//        //    String ip =  new Random().nextInt(20)+"";
//        //    if (map.get(ip) == null) {
//        //        map.put(ip, "add");
//        //        System.out.println(ip +" value aaaa");
//        //    } else {
//        //        ipi = map.get(ip);
//        //        System.out.println(ip +" value "+ipi);
//        //
//        //        if (map.size() > 10) {
//        //            map.clear();
//        //            System.out.println("map 清空啦");
//        //        }
//        //    }
//        //    i++;
//        //}
//        //System.out.println(map.size());
//        //SparkConf sparkConf = new SparkConf()
//        //        .setAppName("ESNStreamingProject")
//        //        .setMaster("local[2]");
//        //try {
//        //JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        //
//        //JavaRDD<String> rdd = sc.textFile("file:////Users/chenxiaolei/t105.txt");
//        //    long count = rdd.filter(new Function<String, Boolean>() {
//        //        @Override
//        //        public Boolean call(String v1) throws Exception {
//        //            return v1.contains("&&");
//        //        }
//        //    }).map(new Function<String, String>() {
//        //
//        //        @Override
//        //        public String call(String v1) throws Exception {
//        //            return v1.split("&&")[0];
//        //        }
//        //    }).mapToPair(new PairFunction<String, String, Integer>() {
//        //        @Override
//        //        public Tuple2<String, Integer> call(String s) throws Exception {
//        //            return new Tuple2<String, Integer>(s, 1);
//        //        }
//        //    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//        //        @Override
//        //        public Integer call(Integer v1, Integer v2) throws Exception {
//        //            return v1 + v2;
//        //        }
//        //    }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
//        //        @Override
//        //        public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//        //            return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
//        //        }
//        //    }).sortByKey(false).filter(new Function<Tuple2<Integer, String>, Boolean>() {
//        //        @Override
//        //        public Boolean call(Tuple2<Integer, String> v1) throws Exception {
//        //            return v1._1 > 1;
//        //        }
//        //    }).count();
//        //    System.out.println(count);
//        //
//        //}catch (Exception e){
//        //    System.out.println("hahahahahahahahaahahahah");
//        //}
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
