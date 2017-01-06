package com.yonyou.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * 日期时间工具类
 * Created by ChenXiaoLei on 2016/11/7.
 */
public class DateUtils {

    public static final SimpleDateFormat TIME_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat DATE_FORMAT =
            new SimpleDateFormat("yyyy:MM:dd");
    public static final SimpleDateFormat DATE_FORMAT2 =
            new SimpleDateFormat("yyyy:MM:dd:HH");
    public static final SimpleDateFormat DATEKEY_FORMAT =
            new SimpleDateFormat("yyyyMMdd");
    public static final SimpleDateFormat DATEKEY2_FORMAT =
            new SimpleDateFormat("mm");

    /**
     * 判断一个时间是否在另一个时间之前
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean before(String time1, String time2) {
        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);

            if (dateTime1.before(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 判断一个时间是否在另一个时间之后
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean after(String time1, String time2) {
        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);
            if (dateTime1.after(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 计算时间差值（单位为秒）
     *
     * @param time1 时间1
     * @param time2 时间2
     * @return 差值
     */
    public static int minus(String time1, String time2) {
        try {
            Date datetime1 = TIME_FORMAT.parse(time1);
            Date datetime2 = TIME_FORMAT.parse(time2);

            long millisecond = datetime1.getTime() - datetime2.getTime();

            return Integer.valueOf(String.valueOf(millisecond / 1000));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取年月日和小时
     *
     * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
     * @return 结果（yyyy-MM-dd_HH）
     */
    public static String getDateHour(String datetime) {
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" + hour;
    }

    /**
     * 获取当天日期（yyyy-MM-dd）
     *
     * @return 当天日期
     */
    public static String getTodayDate() {
        return DATE_FORMAT.format(new Date());
    }
    /**
     * 获取当天日期（yyyy-MM-dd hhmm）
     *
     * @return 当天日期
     */
    public static String getTodayTime() {
        return DATEKEY2_FORMAT.format(new Date());
    }

    /**
     * 获取昨天的日期（yyyy-MM-dd）
     *
     * @return 昨天的日期
     */
    public static String getYesterdayDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -1);

        Date date = cal.getTime();

        return DATE_FORMAT.format(date);
    }

    /**
     * 获取昨天的日期（yyyy-MM-dd-HH）
     *
     * @return 昨天的日期
     */
    public static String getlasthourDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.HOUR_OF_DAY, -1);
        Date date = cal.getTime();
        return DATE_FORMAT2.format(date);
    }

    /**
     * 格式化日期（yyyy-MM-dd）
     *
     * @param date Date对象
     * @return 格式化后的日期
     */
    public static String formatDate(Date date) {
        return DATE_FORMAT.format(date);
    }

    /**
     * 格式化时间（yyyy-MM-dd HH:mm:ss）
     *
     * @param date Date对象
     * @return 格式化后的时间
     */
    public static String formatTime(Date date) {
        return TIME_FORMAT.format(date);
    }

    /**
     * 解析时间字符串
     *
     * @param time 时间字符串
     * @return Date
     */
    public static Date parseTime(String time) {
        try {
            return TIME_FORMAT.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化日期key
     *
     * @param date
     * @return
     */
    public static String formatDateKey(Date date) {
        return DATEKEY_FORMAT.format(date);
    }

    /**
     * 格式化日期key
     *
     * @param datekey
     * @return
     */
    public static Date parseDateKey(String datekey) {
        try {
            return DATEKEY_FORMAT.parse(datekey);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 格式化日期key
     *
     * @param
     * @return
     */
    public static String parseDate(String datekey) {
        Calendar c = Calendar.getInstance();
        Date date=null;
        try {
            date = DATE_FORMAT.parse(datekey);
            c.setTime(date);
            int day=c.get(Calendar.DATE);
            c.set(Calendar.DATE,day);
            DATE_FORMAT.format(c.getTime());
            return DATE_FORMAT.format(c.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化时间，保留到分钟级别
     * yyyyMMddHHmm
     *
     * @param date
     * @return
     */
    public static String formatTimeMinute(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(date);
    }

    public static String getTime(String timestamp) {
        String strDateTime = timestamp.replace("[", "").replace("]", "");
        long datekey = 0l;
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat day = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat hour = new SimpleDateFormat("yyyy-MM-dd HH");
        Date t = null;
        String format = "";
        try {
            t = formatter.parse(strDateTime);
            System.out.print("");
            format = day.format(t);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return format;
    }

    public static long timeStamp2Date(Long seconds, String format) {
        long time = 0l;
        if (seconds == null || seconds.equals("null")) {
            return 0l;
        }
        try {
            if (format == null || format.isEmpty())
                format = "yyyy-MM-dd";
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            String format1 = sdf.format(new Date(seconds));
            Date parse = sdf.parse(format1);
            time = parse.getTime() / 1000;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }
    //根据log信息 转换成当前所在周的周一
    public static String getWeekTime(String timestamp) {
        SimpleDateFormat day = new SimpleDateFormat("yyyy:MM:dd");
        Date t = null;
        String format = "";
        try {
            t = day.parse(timestamp);
            Calendar cal = Calendar.getInstance();
            cal.setTime(t);
            cal.setFirstDayOfWeek(Calendar.MONDAY);//设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一
            int days = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天
            cal.add(Calendar.DATE, cal.getFirstDayOfWeek()-days);//根据日历的规则，给当前日期减去星期几与一个星期第一天的差值
            format = day.format(cal.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return format;
    }
    //转换成根据log信息 转换成当月的第一天
    public static String getMonthTime(String timestamp) {
        SimpleDateFormat day = new SimpleDateFormat("yyyy:MM:dd");
        Date t = null;
        String format = "";
        try {
            t = day.parse(timestamp);
            Calendar cal = Calendar.getInstance();
            cal.setTime(t);
            cal.setFirstDayOfWeek(Calendar.MONDAY);//设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一
            cal.set(Calendar.DAY_OF_MONTH, 1);//设置为1号,当前日期既为本月第一天
            format = day.format(cal.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return format;
    }
   //字符串时间 转换秒级时间戳
    public static String getTimestamp(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy:MM:dd");
        Date date = null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long l = date.getTime() / 1000;
        return l+"";
    }
    public static void main(String[] args)
    {
        //JSONObject jsonObject = JSONObject.parseObject("{\"action\":\"view\",\"app_id\":\"22239\",\"client\":\"android\",\"client_ip\":\"123.1.4.5\",\"device_model\":\"SM-G9250\",\"device_name\":\"三星\",\"instance_id\":\"4785\",\"member_id\":\"3469\",\"mtime\":\"1482134768290\",\"object_id\":\"123456789\",\"qz_id\":\"74269\",\"user_id\":\"0\",\"ver_code\":\"3.0.5\"}");
        //JSONObject s = JSONObject.parseObject("{\"action\":\"view\",\"app_id\":\"99999\",\"client\":\"android\",\"client_ip\":\"123.1.4.5\",\"device_model\":\"SM-G9250\",\"device_name\":\"三星\",\"instance_id\":\"128261\",\"member_id\":\"3469\",\"mtime\":\"1482137412506\",\"object_id\":\"6123456\",\"qz_id\":\"88888\",\"user_id\":\"0\",\"ver_code\":\"3.0.5\"}");
        //Long mtime = s.getLong("mtime");
        //System.out.println(timeStamp2Date(mtime,null));
        //System.out.println(getWeekTime("2016:12:19"));
        //System.out.println(getMonthTime("2016:12:19"));
        //System.out.println(getTimestamp(getWeekTime("2016:12:19")));
        //System.out.println(getTimestamp(getMonthTime("2016:12:19")));
        System.out.println(parseDate("2016:11:30"));
        System.out.println(getTimestamp(parseDate("2016:11:30")));
        System.out.println(getlasthourDate());

    }
}
