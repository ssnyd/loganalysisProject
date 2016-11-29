//package com.yonyou;
//
//import com.alibaba.fastjson.JSONObject;
//import com.google.common.collect.Lists;
////import com.yonyou.hbaseUtil.HBaseCounter;
//
//import java.util.ArrayList;
//import java.util.Set;
//import java.util.regex.Pattern;
//import java.net.*;
//
///**
// * Created by ChenXiaoLei on 2016/11/1.
// */
//public class TestJson {
//    public static void main(String[] args) {
//        String token = "";
//        String s = "61.50.248.5\t-\t10.163.234.94\tapi\t346188062\t1\t939\t[01/Nov/2016:10:46:06 +0800]\t1477968366.938\t\"POST /rest/group/getGroupUpdateInfos HTTP/1.1\"\t{\\x22ek\\x22:\\x221f61d69f0c4b3f95e199f2704f6520b0\\x22,\\x22access_token\\x22:\\x22202974b0f3ac6aa49dee38a9b4e95a923c839433\\x22,\\x22qz_id\\x22:\\x2274269\\x22,\\x22et\\x22:\\x221477968365991\\x22,\\x22gids\\x22:\\x2215788\\x22,\\x22vercode\\x22:\\x222-3.0.3-1-1\\x22}\t200\t286\t503\t\"-\"\t\"esn/3.0.3 (iPhone; iOS 9.3.5; Scale/3.00)\"\t\"-\"\t0.129\t0.088\t\"-\"";
//        String[] lines = s.split("\t");
//        if("api".equals(lines[3])){
//            token = Token(lines);
//            if (token.equals("")){
//                try {
//                    if(lines[9].split(" ").length >= 3){
//                        URL aURL = new URL("http://test.com" + lines[9].split(" ")[1]);
//                        if (!("".equals(aURL.getQuery()))&&aURL.getQuery().contains("token")){
//                            String[] str = aURL.getQuery().split("&");
//                            for (int i = 0; i < str.length; i++) {
//                                if ("access_token".equals(str[i].split("=")[0])){
//                                    token = str[i].split("=")[1];
//                                    break;
//                                }
//                            }
//                        }
//                    }
//                } catch (MalformedURLException e) {
//                    e.printStackTrace();
//                }
//            }
//            System.out.println(token);
//        }
//    }
//
////        HBaseCounter instance = HBaseCounter.getInstance("chen", "info");
////        instance.add("1","chen","tingting");
//
//
////        String s ="61.50.99.26\t-\t10.163.234.94\tapi\t346192022\t5\t911\t[01/Nov/2016:10:48:38 +0800]\t1477968518.171\t\"POST /rest/app/banner HTTP/1.1\"\t{\\x22ek\\x22:\\x227d92170db95ec491aee32d5a5c74f598\\x22,\\x22access_token\\x22:\\x22c6a5ebd7ebcda3641a303a1279aa89498ae19e58\\x22,\\x22qz_id\\x22:\\x225417\\x22,\\x22et\\x22:\\x221477968517912\\x22,\\x22keepsilent\\x22:\\x22yes\\x22,\\x22vercode\\x22:\\x222-3.0.2-1-1\\x22}\t200\t277\t494\t\"-\"\t\"esn/3.0.2 (iPhone; iOS 10.1; Scale/3.00)\"\t\"-\"\t0.099\t0.090\t\"-\"";
////        System.out.println(s.split("\t")[10].replace("\\x22", "\"").replace("\"",""));
////        String s1 = s.split("\t")[10].replace("\\x22", "\"").replace("\"","");
////        String[] split = s1.split(",");
////        for (int i =0 ;i<split.length;i++){
////            if(split[i].contains("token")){
////                String s2 = split[i].split(":")[1];
////                System.out.println(s2);
////                break;
////            }
////        }
//
//
////        System.out.println(s.split("\t")[10].replace("\\x22", "\"").replace("\\x5C", "").replace("\"param\":\"", "\"param\":").replaceAll("\"}$", "}"));
////        System.out.println(JSONObject.parseObject(s.split("\t")[10].replace("\\x22", "\"").replace("\\x5C", "").replace("\"param\":\"", "\"param\":").replaceAll("\"}$", "}")).getString("access_token"));
////// System.out.println(s.split("\t")[10].replace("\\x22", "\""));
////        System.out.println(s.split("\t")[10].replace("\\x22", "\"").replace("\\x5C", ""));
////        String token = JSONObject.parseObject(s.split("\t")[10].replace("\\x22", "\"").replace("\\x5C", "")).getString("token");
////        System.out.println(token);
////        Pattern compile = Pattern.compile(" ");
////        String s1 =  s.split("\t")[9];
////        try {
////            URL aURL = new URL("http://test.com" + s1.split(" ")[1]);
////            String[] str = aURL.getQuery().split("&");
////            for (int i = 0; i < str.length; i++) {
////                if ("access_token".equals(str[i].split("=")[0])){
////                   System.out.println(str[i].split("=")[1]);
////                    return;
////                }
////            }
////        } catch (MalformedURLException e) {
////            e.printStackTrace();
////        }
//
//
////        String replace = s1.replace("\\x22", "\"");
////        System.out.println(replace);
////        JSONObject jsonObject = JSONObject.parseObject(replace);
////        String _result = jsonObject.getString("access_token");
////        System.out.println(_result);
//
////               String s2 =  s.split("\t")[9];
//
////        System.out.println(s1);
//
////        System.out.println((!(s.contains(".js")||s.contains(".css"))) && s.split("\t").length == 20);
//
////        String str = "fdfdsfregefg";
////        boolean fr = str.contains("fr");
////        System.out.println(fr);
//////        Pattern compile = Pattern.compile(" ");
//////        String[] split = compile.split("chen lei xiao");
//////        ArrayList<String> strings = Lists.newArrayList(compile.split("chen lei xiao lei"));
//////        for (int i =0 ;i<strings.size();i++){
//////            System.out.println(strings.get(i));
//////        }
////
//////        String str = "10.144.22.238\t-\t10.163.234.94\tapi\t346188060\t1\t363\t[01/Nov/2016:10:46:06 +0800]\t1477968366.678\t\"POST /rest/scrmFrontPage/summary?access_token=5a96bc6ada37107b3a1423ea2d09bc36e05e7995&v=2.2.5.2&vercode=2-2.2.5-1-2 HTTP/1.0\"\t{\\x22param\\x22:\\x22{\\x5C\\x22type\\x5C\\x22:1}\\x22}\t200\t281\t443\t\"-\"\t\"okhttp/2.4.0\"\t\"223.104.1.18\"\t0.185\t0.184\t\"-\"";
//////        String[] split = str.split("\t");
//////        int length = split.length;
//////        for (int i =0 ;i<length;i++){
//////           String s = split[i];
//////            System.out.println(i+"====="+s);
//////        }
////
////
////
////        String s = "{\\x22timestamp\\x22:\\x221477968111743\\x22,\\x22v\\x22:\\x221.0\\x22,\\x22token\\x22:\\x2218ffd0aec855d5c11b73cb48d0d4e3dbc4005fb9\\x22,\\x22sign\\x22:\\x22872abfbedc8421df37ac0cdc38de6f1a\\x22}";
////        String replace = s.replace("\\x22", "\"");
//////        System.out.println(replace);
////        JSONObject jsonObject = JSONObject.parseObject(replace);
////        String _result = jsonObject.getString("token");
////        System.out.println(_result);
//
//
//
//    private static String Token(String[] lines) {
//        String _token = "";
//        String s1 = lines[10].replace("\\x22", "\"").replace("\"","");
//        String[] split = s1.split(",");
//        for (int i =0 ;i<split.length;i++){
//            if(split[i].contains("token")){
//                _token = split[i].split(":")[1];
//                break;
//            }
//        }
//        return _token;
//    }
//}
