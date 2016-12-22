//package com.yonyou.myTest;
//
//import com.yonyou.utils.IdCrypt;
//
///**
// * Created by chenxiaolei on 16/12/22.
// */
//public class Test1222 {
//    public static void main(String[] args)
//
//    {
//        String str = "\"qz_id=80954148&instance_id=82002670&member_id=83444281\"";
//
//        int i =1;
//            String s =  getOpenApi(str);
//            System.out.println(s+"==="+i);
//
//    }
//    private static String getOpenApi(String line) {
//
//        //首先判断是否20字段为qz_id=80298882&instance_id=78136078&member_id=68175624类型
//        String[] mqu20 = line.replace("\"","").split("&");
//        String qz = "qz_id:empty";
//        String ins = "instance_id:empty";
//        String mem = "member_id:empty";
//        //判断是否长度3 user 设置为0
//        if (mqu20.length == 3) {
//            for (String mqu : mqu20) {
//                String[] mqus = mqu.split("=");
//                if (mqus.length == 2) {
//                    String key = mqus[0];
//                    String value = IdCrypt.decodeId(mqus[1]);
//                    if ("qz_id".equals(key)) {
//                        qz = key + ":" + value;
//                    } else if ("instance_id".equals(key)) {
//                        ins = key + ":" + value;
//                    } else if ("member_id".equals(key)) {
//                        mem = key + ":" + value;
//                    }
//                }
//            }
//            StringBuffer res = new StringBuffer();
//            res.append(mem).append("\t").append(qz).append("\t").append("user_id:empty\t").append(ins);
//            return res.toString();
//        } else {
//            return "member_id:empty\tqz_id:empty\tuser_id:empty\tinstance_id:empty";
//        }
//    }
//}
