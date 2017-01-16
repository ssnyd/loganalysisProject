package com.yonyou.utils;

//com.yonyou.utils.HttpReqUtil

import com.yonyou.conf.ConfigurationManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by ChenXiaoLei on 2016/11/7.
 */
public class HttpReqUtil {
    public static void main(String[] args) {
//    long time = new Date().getTime();
//    System.out.println(time);
//    String result = getResult("user/redis/esn/ft09kavt9b3pkprsf503abtg93", "");
//    System.out.println(result);
//    long time1 = new Date().getTime();
//    System.out.println(time1 - time);
//    System.out.println();
//    String result1 = getResult("ip/query", "59.46.21.54");
//    System.out.println(result1);
        String opid = JSONUtil.getopenId(HttpReqUtil.getResult("app/info/" + 55, ""));
        System.out.println(opid);
    }

    public static String getResult(String mode, String param) {
        URL url = null;
        String res = "";
        HttpURLConnection conn = null;
        try {
            mode = mode + "?secret=" + ConfigurationManager.getProperty("esn_api_secret") + "&app=" + ConfigurationManager.getProperty("esn_api_app");
            url = new URL(ConfigurationManager.getProperty("esn_api_addr") + mode);
            conn = (HttpURLConnection) url.openConnection();
            if (param.length() != 0) {
                conn.setRequestMethod("POST");
            } else {
                conn.setRequestMethod("GET");
            }
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            conn.setRequestProperty("Host", ConfigurationManager.getProperty("esn_api_host"));
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(2000);
            conn.setDoOutput(true);
            StringBuffer params = new StringBuffer();
            if (param.length() != 0) {
                params.append("ip_list").append("=").append("[\"").append(param).append("\"]");
                byte[] bypes = params.toString().getBytes();
                conn.getOutputStream().write(bypes);
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            String line;
            while ((line = in.readLine()) != null) {
                res = res + line;
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.disconnect();
        }
        return res;
    }
}
