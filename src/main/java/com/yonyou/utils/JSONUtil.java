package com.yonyou.utils;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by ChenXiaoLei on 2016/11/1.
 */
public class JSONUtil {
    private static String ecode ;
    private static String result;
    //member_id
    private static String member_id;
    //qz_id
    private static String qz_id;
    //user_id
    private static String user_id;
    //instance_id
    private static String instance_id;
    //country
    private static String country;
    //region
    private static String region;
    //city
    private static String city;
    /**
     *
     * @param resultjson http 返回得JSON字符串
     * @return
     */
    public static String getmquStr(String resultjson){
        JSONObject jsonObject = JSONObject.parseObject(resultjson);
        ecode = jsonObject.getString("ecode");
        if ("0".equals(ecode)){
            result = jsonObject.getString("result");
            member_id = JSONObject.parseObject(result).getString("member_id");
            qz_id = JSONObject.parseObject(result).getString("qz_id");
            user_id = JSONObject.parseObject(result).getString("user_id");
            instance_id = JSONObject.parseObject(result).getString("instance_id");
            return "member_id:"+member_id+"\tqz_id:"+qz_id+"\tuser_id:"+user_id+"\tinstance_id:"+instance_id;
        }else {
            return "member_id:empty\tqz_id:empty\tuser_id:empty\tinstance_id:empty";
        }
    }

    /**
     *
     * @param resultjson
     * @return
     */
    public static String getappId(String resultjson){
        //{"action":"view","app_id":"22239","client":"android","client_ip":"10.1.201.131","device_model":"SM-G9250","device_name":"三星","instance_id":"3219","member_id":"3469","mtime":"1480044831884","qz_id":"3968","user_id":"3469","ver_code":"3.0.5"}
        JSONObject jsonObject = JSONObject.parseObject(resultjson);
        String action  = jsonObject.getString("action");
        String app_id = jsonObject.getString("app_id");
        instance_id = jsonObject.getString("instance_id");
        member_id = jsonObject.getString("member_id");
        qz_id = jsonObject.getString("qz_id");
        String mtime = jsonObject.getString("mtime");
        //
        return "action:"+action+"&app_id:"+app_id+"&instance_id:"+instance_id+"&qz_id:"+qz_id+"&member_id:"+member_id+"&mtime:"+mtime;
    }
    public static String getMember(String resultjson){
        //{"action":"view","app_id":"22239","client":"android","client_ip":"10.1.201.131","device_model":"SM-G9250","device_name":"三星","instance_id":"3219","member_id":"3469","mtime":"1480044831884","qz_id":"3968","user_id":"3469","ver_code":"3.0.5"}
        JSONObject jsonObject = JSONObject.parseObject(resultjson);
        String action  = jsonObject.getString("action");
        member_id = jsonObject.getString("member_id");
        String mtime = jsonObject.getString("mtime");
        return "action:"+action+"&member_id:"+member_id+"&mtime:"+mtime;
    }
    public static String getmTime(String resultjson){
        JSONObject jsonObject = JSONObject.parseObject(resultjson);
        String mtime = "";
               mtime =  jsonObject.getString("mtime");
        return mtime;
    }

    /**
     *
     * @param ipjson http 返回得JSON字符串
     * @return
     */
    public static String getIPStr(String ipjson){
        JSONObject jsonObject = JSONObject.parseObject(ipjson);
        ecode = jsonObject.getString("ecode");
        if ("0".equals(ecode)){
            String ip = JSONObject.parseObject(jsonObject.getString("result")).keySet().toString();
            result = JSONObject.parseObject(jsonObject.getString("result")).getString(ip.substring(1,ip.length()-1));
            country = JSONObject.parseObject(result).getString("country");
            region = JSONObject.parseObject(result).getString("region");
            city = JSONObject.parseObject(result).getString("city");
            return "country:"+country+"\tregion:"+region+"\tcity:"+city;
        }else {
            return "country:empty\tregion:empty\tcity:empty";
        }
    }
    //{"ecode":"0","error":"","result":{"app_id":"22239","name":"协同日程新","open_appid":"110","qz_id":"3968"}}
    public static String getopenId(String appid){
        try {
            JSONObject jsonObject = JSONObject.parseObject(appid);

            ecode = jsonObject.getString("ecode");
            String name = "";
            String open_appid = "";
            if ("0".equals(ecode)){
                result = jsonObject.getString("result");
                name = JSONObject.parseObject(result).getString("name");
                open_appid = JSONObject.parseObject(result).getString("open_appid");
            }
            return "open_appid:"+open_appid+"&name:"+name;
        } catch (Exception e){
            return "";
        }



    }

    /**
     * 测试 线上注释
     * @param args
     */
    public static void main(String[] args) {
       // String s = JSONUtil.getmquStr("{\"ecode\":\"0\",\"error\":\"\",\"result\":{\"member_id\":\"3469\"}}");
//        String s1 = JSONUtil.getIPStr("{\"ecode\":\"0\",\"error\":\"\",\"result\":{\"59.46.21.54\":{\"city\":\"沈阳\",\"country\":\"中国\",\"region\":\"辽宁\"}}}");
//        System.out.println(s1);
//        String s = "{\"memberid\":\"2834488\",\"es\":\"1413131649\",\"q\":\"account/invite/qzInvite\",\"upesntype\":\"joinQz\",\"qzName\":\"\\xE5\\xB7\\xB4\\xE5\\x8E\\x98\\xE5\\xB2\\x9B\\xE6\\x9D\\xA5\\xE5\\x95\\xA6\",\"model\":\"qrCode\",\"uid\":\"2834538\",\"qzid\":\"90708\"}";
//        JSONObject jsonObject = JSONObject.parseObject(s);
//        Byte qzName = jsonObject.getByte("qzName");
//        System.out.println(Byte.toString(qzName));
//        System.out.println(getappId("{\"action\":\"view\",\"app_id\":\"22239\",\"client\":\"android\",\"client_ip\":\"10.1.201.131\",\"device_model\":\"SM-G9250\",\"device_name\":\"三星\",\"instance_id\":\"3219\",\"member_id\":\"3469\",\"mtime\":\"1480044831884\",\"qz_id\":\"3968\",\"user_id\":\"3469\",\"ver_code\":\"3.0.5\"}"));
        System.out.println(getopenId("{\"ecode\":\"0\",\"error\":\"\",\"result\":{\"app_id\":\"22239\",\"name\":\"协同日程新\",\"open_appid\":\"110\",\"qz_id\":\"3968\"}}"));



    }
}
