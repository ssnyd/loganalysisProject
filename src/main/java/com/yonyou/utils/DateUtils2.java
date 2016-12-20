package com.yonyou.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * 时间工具类2
 * Created by chenxiaolei on 16/12/13.
 */
public class DateUtils2 {
    public static long getTime(String timestamp , int num ) {
        String strDateTime = timestamp.replace("[", "").replace("]", "");
        long datekey = 0l;
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat day = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd HH");
        Date t = null;
        String format = "";
        try {
            t = formatter.parse(strDateTime);
            if (1 == num ){
                format = day.format(t);
                t = day.parse(format);
            } else if (2 == num) {
                format = hour.format(t);
                t = hour.parse(format);
            }
            datekey = t.getTime() / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return datekey;
    }
    public static long getTime(String timestamp) {
        String strDateTime = timestamp.replace("[", "").replace("]", "");
        long datekey = 0l;
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat day = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date t = null;
        String format = "";
        try {
            t = formatter.parse(strDateTime);
            format = day.format(t);
            t = day.parse(format);
            datekey = t.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return datekey;
    }
    public static long getDayLong(long time) {
        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd");
        long day =0l;
        try {
            String format = hour.format(time*1000);
            Date date = new Date(time * 1000);

            Date parse = hour.parse(format);
            day = parse.getTime() / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return day;
    }
    public static String getDayTime(String timestamp) {
        String strDateTime = timestamp.replace("[", "").replace("]", "");
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat hour = new SimpleDateFormat("yyyy:MM:dd");
        Date parse = null;
        try {
            parse = formatter.parse(strDateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return hour.format(parse);
    }
    public static String getWeek() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd");
        //获取当前周
        Calendar cal = Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);//将每周第一天设为星期一，默认是星期天
        cal.add(Calendar.DATE, 0);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        String monday = format.format(cal.getTime());
        Date t  = null;
        try {
            t = format.parse(monday);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long datekey = t.getTime()/1000;
        return datekey+"";
    }
    public static String getWeeks() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd");
        //获取当前周
        Calendar cal = Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);//将每周第一天设为星期一，默认是星期天
        cal.add(Calendar.DATE, 0);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        String monday = format.format(cal.getTime());
        return monday;
    }
    public static String getMonth(){
        SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd");
        //获取当前周
        //获取当前月第一天：
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MONTH, 0);
        c.set(Calendar.DAY_OF_MONTH, 1);//设置为1号,当前日期既为本月第一天
        String first = format.format(c.getTime());
        Date t  = null;
        try {
            t = format.parse(first);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long datekey = t.getTime()/1000;


        return datekey+"";
    }

    public static void main(String[] args) {
        System.out.println(getMonth());
        System.out.println(getWeek());
        System.out.println(getWeeks());
        System.out.println(getDayTime("[16/Dec/2016:15:53:22 +0800]"));
    }
}
