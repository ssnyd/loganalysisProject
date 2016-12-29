package com.yonyou.myTest;

import com.yonyou.utils.DateUtils2;

/**
 * Created by chenxiaolei on 16/12/26.
 */
public class Test1226 {
    public static void main(String[] args) throws InterruptedException {
        //System.out.println(CacheManager.getSimpleFlag("alksd"));
        System.out.println("hello");
        //Cache cache = new Cache();
        String date = "[29/Dec/2016:15:14:12 +0800]";
        long time = DateUtils2.getTime(date, 2);
        System.out.println(time);
        String l = "35.9";
        double d = 0.0;
        try {
            d = Double.parseDouble(l);
            System.out.println(d);


        } catch (Exception e){
            d = 0.5;
            System.out.println(d);
        }

        //cache.setValue("chashfdsgfgfge");
        //CacheManager.putCacheInfo("abc", cache,10l);
        //Cache abc = CacheManager.getCacheInfo("abc");
        //System.out.println(abc.getValue()+"===="+abc.getTimeOut());
        //System.out.println(abc.getValue()+"===="+abc.getTimeOut());
        //Thread.sleep(1000);
        //System.out.println(CacheManager.getCacheInfo("abc").getValue()+"===="+CacheManager.getCacheInfo("abc").getTimeOut());
        //System.out.println(abc.getValue()+"===="+abc.getTimeOut());
        //Thread.sleep(1000);
        //
        //System.out.println(CacheManager.getCacheInfo("abc").getValue()+"===="+CacheManager.getCacheInfo("abc").getTimeOut());
        //System.out.println(abc.getValue()+"===="+abc.getTimeOut());
        //System.out.println(abc.getValue()+"===="+abc.getTimeOut());
        //Thread.sleep(1000);
        //
        //System.out.println(CacheManager.getCacheInfo("abc").getValue()+"===="+CacheManager.getCacheInfo("abc").getTimeOut());
        //System.out.println(abc.getValue()+"===="+abc.getTimeOut());
        //System.out.println(abc.getValue()+"===="+abc.getTimeOut());
        //System.out.println(abc.getValue()+"===="+abc.getTimeOut());
        //System.out.println(abc.getValue()+"===="+abc.getTimeOut());
        //HashMap cacheMap = new HashMap();
        //cacheMap.put("a","1");
        //cacheMap.put("b","2");
        //cacheMap.put("c","3");
        //Set set = cacheMap.keySet();
        //int i =1;
        //System.out.println(cacheMap.get("a"));
        //System.out.println(cacheMap.get("b"));
        //System.out.println(cacheMap.get("c"));
        //String[] str = new String[3];
        //for (Object o :set){
        //    str[i]=(String) o;
        //    i++;
        //    if (i>2){
        //        break;
        //    }
        //}
        //for (String s :str){
        //    cacheMap.remove(s);
        //}
        //System.out.println(cacheMap.size());
        //System.out.println(cacheMap.get("a"));
        //System.out.println(cacheMap.get("b"));
        //System.out.println(cacheMap.get("c"));
        //System.out.println(cacheMap.get("a"));
        //System.out.println(cacheMap.get("b"));
        //System.out.println(cacheMap.get("c"));



    }
}
