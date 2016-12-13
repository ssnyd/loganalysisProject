//package com.yonyou.myTest;
//
//
//import com.yonyou.newProjectApi.ESNStreaming2HbaseOptimize;
//import com.yonyou.utils.HttpReqUtil;
//import com.yonyou.utils.JSONUtil;
//
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.text.ParseException;
//
//public class test1{
//    public static void main(String[] args) throws ParseException {
//        String s = "222.134.55.245\t-\t10.252.18.52\th5-api\t424534604\t3\t639\t[13/Dec/2016:14:25:39 +0800]\t1481610339.401\t\"GET /reply/getList?limit=10&obj_id=373757&obj_type=190&timestamp=1481610338715&token=fa83628377a241694d80dcc8d0f904334c4d5de0&v=1.0&sign=aff1b75b1104a57d648a61050d930b79 HTTP/1.1\"\t{\\x22obj_type\\x22:\\x22190\\x22,\\x22limit\\x22:\\x2210\\x22,\\x22sign\\x22:\\x22aff1b75b1104a57d648a61050d930b79\\x22,\\x22timestamp\\x22:\\x221481610338715\\x22,\\x22v\\x22:\\x221.0\\x22,\\x22token\\x22:\\x22fa83628377a241694d80dcc8d0f904334c4d5de0\\x22,\\x22obj_id\\x22:\\x22373757\\x22}\t200\t95\t545\t\"https://m.upesn.com/dist/journal/index.html\"\t\"Mozilla/5.0 (Linux; Android 4.4.4; SM-A5000 Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 QYZone\"\t\"-\"\t0.140\t0.140\t\"-\"";
//        System.out.println(s.split("\t").length);
//        String token = "";
//        String[] lines = s.split("\t");
//        String mquID = "member_id:empty\tqz_id:empty\tuser_id:empty";
//        String remote_addr = "country:empty\tregion:empty\tcity:empty\tinstance_id:empty";
//        //求ip
//        String ip = lines[0];
//        if (!"".equals(ip) && ip.length() < 16) {//判断IP不脏
//            String result = "";
//            try {
//                result = HttpReqUtil.getResult("ip/query", ip);
//                try {
//                    //调用PHP接口，PHP返回json串，解析该json串，得到eg：
//                    //country:"+country+"\tregion:"+region+"\tcity:"+city的字符串
//                    remote_addr = JSONUtil.getIPStr(result);
//                } catch (Exception e) {
//                    System.out.println(result + " ==> 解析 ip → json 错误");
//                    System.out.println(ip + " ==> json→ip");
//                }
//            } catch (Exception e) {
//                System.out.println(ip + " ==> read time out! 数据跳过");
//            }
//        }
//        //求mqu
//        if (lines[9].contains(".htm")) {//RDD中的一行中包含".htm"，便不予处理
//            System.out.println("存在htm 不分析");
//        } else if ("esn".equals(lines[3])) {
//            //lines[19]eg："PHPSESSID=67ivh3pr0oq32hh17p5klokrk5; path=/"
//            if (lines[19].split(" ")[0].contains("PHPSESSID")) {
//                token = lines[19].split(" ")[0].split("=")[1].split(";")[0];
//            }
//            if (!"".equals(token) && !token.contains("/") && !token.contains("\\")) {
//                String result = "";
//                try {
//                    result = HttpReqUtil.getResult("user/redis/esn/" + token, "");
//                    //调用PHP接口，PHP返回json串，解析该json串，得到eg：
//                    //"member_id:"+member_id+"\tqz_id:"+qz_id+"\tuser_id:"+user_id+"\tinstance_id:"+instance_id
//                    mquID = JSONUtil.getmquStr(result);
//                } catch (Exception e) {
//                    System.out.println(token + "read time out ! token数据跳过");
//                    System.out.println(result + " ==> json→token");
//                }
//            }
//        } else {
//            if ("api".equals(lines[3])) {
//                token = ESNStreaming2HbaseOptimize.Token(lines);
//                if (token.equals(""))
//                    try {
//                        //lines[9]eg:
//                        //"POST /rest/app/getList3 HTTP/1.1"
//                        //"GET /rest/user/getMyActiveInfo?access_token=f7dccdf70b53297a5ce9dc63e637a4c56c289e0a&appType=1&ek=b8bdffa4b6690fe7ccdc5bb12a64be36&et=1480398779&member_id=198659&qz_id=4243&vercode=1-3.0.6-1-1 HTTP/1.1"
//                        //判断GET请求
//                        if ((lines[9].split(" ").length >= 3) && (lines[9].contains("?")) && (lines[9].contains("token"))) {
//                            URL aURL = new URL("http://test.com" + lines[9].split(" ")[1]);
//                            //拼接后的aURL:
//                            //eg:http://test.com/rest/user/getMyActiveInfo?access_token=f7dccdf70b53297a5ce9dc63e637a4c56c289e0a&appType=1&ek=b8bdffa4b6690fe7ccdc5bb12a64be36&et=1480398779&member_id=198659&qz_id=4243&vercode=1-3.0.6-1-1 HTTP/1.1
//                            if ((!"".equals(aURL.getQuery())) && (aURL.getQuery().contains("token"))) {
//                                String[] str = aURL.getQuery().split("&");
//                                for (int i = 0; i < str.length; i++)
//                                    if ("access_token".equals(str[i].split("=")[0]) && str[i].split("=").length == 2) {
//                                        token = str[i].split("=")[1];
//                                        break;
//                                    }
//                            }
//                        }
//                    } catch (MalformedURLException e) {
//                        e.printStackTrace();
//                    }
//            } else {
//                token = ESNStreaming2HbaseOptimize.Token(lines);
//                if ("".equals(token)) {
//                    try {
//                        if ((lines[9].split(" ").length >= 3) && (lines[9].contains("?")) && (lines[9].contains("token"))) {
//                            URL aURL = new URL("http://test.com" + lines[9].split(" ")[1]);
//                            if ((!"".equals(aURL.getQuery())) && (aURL.getQuery().contains("token"))) {
//                                String[] str = aURL.getQuery().split("&");
//                                for (int i = 0; i < str.length; i++)
//                                    if ("token".equals(str[i].split("=")[0]) && str[i].split("=").length == 2) {
//                                        token = str[i].split("=")[1];
//                                        break;
//                                    }
//                            }
//                        }
//                    } catch (MalformedURLException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//            if (!"".equals(token) && !token.contains("/") && !token.contains("\\")) {
//                //调用PHP接口，PHP返回json串，解析该json串，得到eg：
//                //"member_id:"+member_id+"\tqz_id:"+qz_id+"\tuser_id:"+user_id+"\tinstance_id:"+instance_id
//                mquID = JSONUtil.getmquStr(HttpReqUtil.getResult("user/redis/api/" + token, ""));
//            }
//        }
//        if (!remote_addr.contains("局域网")) {
//            String value = s + "\t" + remote_addr + "\t" + mquID;
//            //eg:lines[7]
//            //[29/Nov/2016:13:52:50 +0800]
//            //getTime后格式：yyyy:MM:dd:HH:mm:ss
//            String times = ESNStreaming2HbaseOptimize.getTime(lines[7]);
//        }
//        System.out.println(remote_addr+"\t"+mquID);
//        System.out.println(token);
//
//
//
//
//
//
//
//        //for (int i = 0 ;i<10000;i++) {
//        //    if ((int) (Math.random() * (4)) > 3) {
//        //        System.out.println((int) (Math.random() * (4)));
//        //    }
//        //}
//        //    System.out.println("完啦");
//        //ApplysStat as = new ApplysStat();
//        //as.setMtime("3423423432");
//        //
//        //
//        //String json = "{\"action\":\""+as.getAction()+"\",\"app_id\":\""+as.getApp_id()+"\",\"client\":\""+as.getClient()+"\",\"client_ip\":\""+as.getClient_ip()+"\",\"device_model\":\""+as.getDevice_model()+"\",\"device_name\":\""+as.getDevice_name()+"\",\"member_id\":\""+as.getMember_id()+"\",\"mtime\":\""+as.getMtime()+"\",\"qz_id\":\""+as.getQz_id()+"\",\"user_id\":\""+as.getUser_id()+"\",\"ver_code\":\""+as.getVer_code()+"\",\"instance_id\":\""+as.getInstance_id()+"\"}";
//        //System.out.println(json);
//        //JSONObject jsonObject = JSONObject.parseObject(json);
//        //        Long mtime = jsonObject.getLong("mtime");
//        //System.out.println(mtime);
//
////        String s = DateUtils.getlasthourDate();
////        System.out.println(s);
////
//
////        String s = "\"Dalvik/2.1.0 (Linux; U; Android 6.0; EVA-AL10 Build/HUAWEIEVA-AL10)\"";
////        long time = getTime("[01/Nov/2016:10:46:09 +0800]");
//////        System.out.println(time);
////        String s = "10.144.22.238\t-\t10.163.234.94\tapi\t346188060\t1\t363\t[01/Nov/2016:10:46:06 +0800]\t1477968366.678\t\"POST /rest/scrmFrontPage/summary?access_token=5a96bc6ada37107b3a1423ea2d09bc36e05e7995&v=2.2.5.2&vercode=2-2.2.5-1-2 HTTP/1.0\"\t{\\x22param\\x22:\\x22{\\x5C\\x22type\\x5C\\x22:1}\\x22}\t200\t281\t443\t\"-\"\t\"okhttp/2.4.0\"\t\"223.104.1.18\"\t0.185\t0.184\t\"-\"";
////        String s1 = "202.99.220.242\t-\t10.163.234.94\tapi\t346188085\t1\t332\t[01/Nov/2016:10:46:07 +0800]\t1477968367.369\t\"GET /rest/qz/getQzListSlip30.json?access_token=5f63ba17ce6344c1b7954303f676244325b51f9d&ek=1242dde0f2976eb310372ced22800096&et=1477968367&qz_id=76964&vercode=1-3.0.2-1-1 HTTP/1.1\"\t{\\x22ek\\x22:\\x221242dde0f2976eb310372ced22800096\\x22,\\x22access_token\\x22:\\x225f63ba17ce6344c1b7954303f676244325b51f9d\\x22,\\x22q\\x22:\\x22rest\\x5C/qz\\x5C/getQzListSlip30.json\\x22,\\x22et\\x22:\\x221477968367\\x22,\\x22vercode\\x22:\\x221-3.0.2-1-1\\x22,\\x22qz_id\\x22:\\x2276964\\x22}\t200\t620\t837\t\"-\"\t\"Dalvik/2.1.0 (Linux; U; Android 6.0.1; MI 4W MIUI/V8.0.2.0.MXDCNDG)\"\t\"-\"\t0.103\t0.103\t\"-\"";
////
//////        String[] split = s1.split("\t");
//////        String s2 = split[15];
//////        boolean android = s2.contains("Android");
//////        System.out.println(android);
////        String s = "{\"action\":\""+"aaa"+"\",\"app_id\":\""+"aaa"+"\",\"client\":\"android\",\"client_ip\":\"123.103.9.8\",\"device_model\":\"SM-G9250\",\"device_name\":\"三星\",\"member_id\":\"87899\",\"mtime\":\"1479692953121\",\"qz_id\":\"74269\",\"user_id\":\"87881\",\"ver_code\":\"3.0.6\",\"instance_id\":\"1234\"}";
////        System.out.println(s);
//////        String s = "1480044831884";
//////        SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd");
//////        Long time=Long.parseLong(s);
//////        String d = format.format(time);
////        Date date=format.parse(d);
////        System.out.println("Format To Date:"+date.getTime()/1000);
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
////        final PVStatQueryResult queryResult = new PVStatQueryResult();
////        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
////        Connection conn = jdbcUtils.getConnection();
////        String selectSQL = "SELECT id "
////                            + "FROM rp_app_general "
////                            + "WHERE appId=? "
////                            + "AND openAppId=? ";
////        String insertSQL = "INSERT INTO rp_app_general(appId,openAppId,name,created) "
////                            + "VALUES(?,?,?,?)";
////        JDBCHelper.executeQuery(conn,selectSQL,new Object[]{
////                "2222","3333"
////        },new JDBCHelper.QueryCallback(){
////                    @Override
////                    public void process(ResultSet rs) throws Exception {
////                        if (rs.next()){
////                            int count = rs.getInt(1);
////                            queryResult.setCount(count);
////                        }
////                    }
////                });
////                int count = queryResult.getCount();
////        if (count == 0) {
////            JDBCHelper.executeUpdate(conn,insertSQL,new Object[]{
////                "2222","3333","签到中心",new Date().getTime()/1000
////            });
////            JDBCHelper.executeQuery(conn,selectSQL,new Object[]{
////                "2222","3333"
////        },new JDBCHelper.QueryCallback(){
////                    @Override
////                    public void process(ResultSet rs) throws Exception {
////                        if (rs.next()){
////                            int count = rs.getInt(1);
////                            queryResult.setCount(count);
////                        }
////                    }
////                });
////            count = queryResult.getCount();
////        }
////
////        System.out.println(count);
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
////        SimpleDateFormat day = new SimpleDateFormat("yyyy:MM:dd");
////        String yesterdayDate = DateUtils.getYesterdayDate();
////        System.out.println(yesterdayDate);
////        Date parse = day.parse(yesterdayDate);
////        long l = parse.getTime() / 1000;
////        System.out.println(l);
//
//
////        long timestamp = getTime("[01/Nov/2016:10:41:53 +0800]", 2);
////        System.out.println(timestamp);
////
//////        System.out.println(DateUtils.getYesterdayDate()+":#");
////        String s = "{\"action\":\"view\",\"app_id\":\"21487\",\"client\":\"android\",\"client_ip\":\"125.35.5.254\",\"device_model\":\"SM-G9250\",\"device_name\":\"三星\",\"member_id\":\"87899\",\"mtime\":\"1478866044104\",\"qz_id\":\"5417\",\"user_id\":\"87881\",\"ver_code\":\"3.0.6\"}";
////        JSONObject jsonObject = JSONObject.parseObject(s);
////        Long mtime = jsonObject.getLong("mtime");
////        System.out.println(DateUtils.timeStamp2Date(mtime,null));
////        System.out.println(UUID.randomUUID().toString().replace("-",""));
////        String fd = "343434";
////        System.out.println(        Long.parseLong(fd));
////        String s = "[01/Nov/2016:10:46:06 +0800]";
////        String strDateTime = s.replace("[", "").replace("]", "");
////        long datekey = 0l;
////        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
////        Date parse = formatter.parse(strDateTime);
////        SimpleDateFormat hour = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss");
////        System.out.println(hour.format(parse));
////        System.out.println(            DateUtils.getYesterdayDate());
//    }
//
//
//
//}
//
//
//
