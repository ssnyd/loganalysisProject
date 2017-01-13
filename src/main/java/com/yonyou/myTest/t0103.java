//package com.yonyou.myTest;
//
//import java.io.Serializable;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.Locale;
//
///**
// * Created by chenxiaolei on 17/1/3.
// */
//public class t0103 implements Serializable{
//    private static final long serialVersionUID = -701633232681493186L;
//    public static void main(String[] args) {
//        //final List<Tuple2<String, Boolean>> list = new ArrayList<Tuple2<String, Boolean>>();
//        //String selectSQL = "select distinct memberId from rp_activity_data where action = 'statisticsRegister' and created >=1481698591 and created <= 1483286400";
//        ////
//        //JDBCUtils jdbcUtils = JDBCUtils.getInstance();
//        //Connection conn = jdbcUtils.getConnection();
//        //JDBCHelper.executeQuery(conn, selectSQL, null, new JDBCHelper.QueryCallback() {
//        //    @Override
//        //    public void process(ResultSet rs) throws Exception {
//        //        while (rs.next()) {
//        //            list.add(new Tuple2<String, Boolean>(rs.getInt(1) + "", true));
//        //        }
//        //    }
//        //});
//        //Map<String, Boolean> whiteMap = new HashMap<String, Boolean>();
//        //for (Tuple2<String, Boolean> memberId : list) {
//        //    whiteMap.put(memberId._1, memberId._2);
//        //}
//        //Boolean s = whiteMap.get("4343");
//        //if (s==null){
//        //    System.out.println(false);
//        //}else {
//        //    System.out.println(s);
//        //}
//        System.out.println(getTime("[07/Jan/2017:23:59:52 +0800]",0));
//
//
//
//
//    }
//    private static long getTime(String timestamp, int num) {
//        String strDateTime = timestamp.replace("[", "").replace("]", "");
//        long datekey = 0l;
//        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
//        SimpleDateFormat day = new SimpleDateFormat("yyyy-MM-dd");
//        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd HH");
//        Date t = null;
//        String format = "";
//        try {
//            t = formatter.parse(strDateTime);
//            if (1 == num) {
//                format = day.format(t);
//                t = day.parse(format);
//            } else if (2 == num) {
//                format = hour.format(t);
//                t = hour.parse(format);
//            }
//            datekey = t.getTime() / 1000;
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return datekey;
//    }
//
//
//}
