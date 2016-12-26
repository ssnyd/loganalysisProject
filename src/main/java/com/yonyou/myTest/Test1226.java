package com.yonyou.myTest;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by chenxiaolei on 16/12/26.
 */
public class Test1226 {
    public static void main(String[] args) throws InterruptedException {
        //System.out.println(CacheManager.getSimpleFlag("alksd"));
        //Cache cache = new Cache();
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
        HashMap cacheMap = new HashMap();
        cacheMap.put("a","1");
        cacheMap.put("b","2");
        cacheMap.put("c","3");
        Set set = cacheMap.keySet();
        int i =1;
        System.out.println(cacheMap.get("a"));
        System.out.println(cacheMap.get("b"));
        System.out.println(cacheMap.get("c"));
        String[] str = new String[3];
        for (Object o :set){
            str[i]=(String) o;
            i++;
            if (i>2){
                break;
            }
        }
        for (String s :str){
            cacheMap.remove(s);
        }
        System.out.println(cacheMap.size());
        System.out.println(cacheMap.get("a"));
        System.out.println(cacheMap.get("b"));
        System.out.println(cacheMap.get("c"));
        //System.out.println(cacheMap.get("a"));
        //System.out.println(cacheMap.get("b"));
        //System.out.println(cacheMap.get("c"));



    }
}
