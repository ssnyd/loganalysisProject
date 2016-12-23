//package com.yonyou.myTest;
//
//import com.yonyou.utils.DateUtils;
//
///**
// * Created by chenxiaolei on 16/12/19.
// *
// */
//public class test1219 {
//    public static void main(String[] args) throws Exception {
//        System.out.println(getCity("四川"));
//
//
//
//
//
//
//        //SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd"); //设置时间格式
//        //Calendar cal = Calendar.getInstance();
//        //Date time=sdf.parse("2016-11-19 14:22:47");
//        //cal.setTime(time);
//        //System.out.println("要计算日期为:"+sdf.format(cal.getTime())); //输出要计算日期
//        //
//        ////判断要计算的日期是否是周日，如果是则减一天计算周六的，否则会出问题，计算到下一周去了
//        //int dayWeek = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天
//        //if(1 == dayWeek) {
//        //    cal.add(Calendar.DAY_OF_MONTH, -1);
//        //}
//        //
//        //cal.setFirstDayOfWeek(Calendar.MONDAY);//设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一
//        //
//        //int day = cal.get(Calendar.DAY_OF_WEEK);//获得当前日期是一个星期的第几天
//        //cal.add(Calendar.DATE, cal.getFirstDayOfWeek()-day);//根据日历的规则，给当前日期减去星期几与一个星期第一天的差值
//        //System.out.println("所在周星期一的日期："+sdf.format(cal.getTime()));
//        //System.out.println(cal.getFirstDayOfWeek()+"-"+day+"+6="+(cal.getFirstDayOfWeek()-day+6));
//        //
//        //cal.add(Calendar.DATE, 6);
//        //System.out.println("所在周星期日的日期："+sdf.format(cal.getTime()));
//        //
//        //cal.set(Calendar.DAY_OF_MONTH, 1);//设置为1号,当前日期既为本月第一天
//        //String firstDay = sdf.format(cal.getTime());
//        //System.out.println("-----1------firstDay:" + firstDay);
//        //
//        //
//        //System.out.println(getKey(new String[]{}));
//
//
//
//    }
//    private static String getCity(String city) {
//        String str = "haiwai";//35
//        if ("澳门".equals(city)) {//36
//            str = "aomen";
//        } else if ("香港".equals(city)) {//1
//            str = "xianggang";
//        } else if ("台湾".equals(city)) {//2
//            str = "taiwan";
//        } else if ("广东".equals(city)) {//3
//            str = "guangdong";
//        } else if ("广西".equals(city)) {//4
//            str = "guangxi";
//        } else if ("海南".equals(city)) {//5
//            str = "hainan";
//        } else if ("云南".equals(city)) {//6
//            str = "yunnan";
//        } else if ("福建".equals(city)) {//7
//            str = "fujian";
//        } else if ("江西".equals(city)) {//8
//            str = "jiangxi";
//        } else if ("湖南".equals(city)) {//9
//            str = "hunan";
//        } else if ("贵州".equals(city)) {//10
//            str = "guizhou";
//        } else if ("浙江".equals(city)) {//11
//            str = "zhejiang";
//        } else if ("安徽".equals(city)) {//12
//            str = "anhui";
//        } else if ("上海".equals(city)) {//13
//            str = "shanghai";
//        } else if ("江苏".equals(city)) {//14
//            str = "jiangsu";
//        } else if ("湖北".equals(city)) {//15
//            str = "hubei";
//        } else if ("西藏".equals(city)) {//16
//            str = "xizang";
//        } else if ("青海".equals(city)) {//17
//            str = "qinghai";
//        } else if ("陕西".equals(city)) {//18
//            str = "shanxi_shan";
//        } else if ("山西".equals(city)) {//19
//            str = "shanxi_jin";
//        } else if ("河南".equals(city)) {//20
//            str = "henan";
//        } else if ("山东".equals(city)) {//21
//            str = "shandong";
//        } else if ("河北".equals(city)) {//22
//            str = "hebei";
//        } else if ("天津".equals(city)) {//23
//            str = "tianjin";
//        } else if ("北京".equals(city)) {//24
//            str = "beijing";
//        } else if ("宁夏".equals(city)) {//25
//            str = "ningxia";
//        } else if ("内蒙古".equals(city)) {//26
//            str = "neimeng";
//        } else if ("辽宁".equals(city)) {//27
//            str = "liaoning";
//        } else if ("吉林".equals(city)) {//28
//            str = "jilin";
//        } else if ("黑龙江".equals(city)) {//29
//            str = "heilongjiang";
//        } else if ("重庆".equals(city)) {//30
//            str = "chongqing";
//        } else if ("四川".equals(city)) {//31
//            str = "sichuan";
//        } else if ("新疆".equals(city)) {//32
//            str = "xinjiang";
//        } else if ("甘肃".equals(city)) {//33
//            str = "gansu";
//        } else if ("局域网".equals(city)) {//34
//            str = "local";
//        }
//        return str;
//    }
//    private static String getKey(String[] s1) {
//        if (s1.length == 2) {
//            return DateUtils.parseDate(s1[0]);
//        } else {
//            return DateUtils.getTodayDate();
//        }
//    }
//}
