package com.yonyou.cxl.cache;


import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 简单的内存缓存实现,实现group概念
 * 一个group里面是个有序的集合
 * 集合支持key-value
 * Created by chenxiaolei on 16/12/30.
 */
public class GroupCacheFactory {

    // 数据容器
    private Map<String, Group> container;

    public GroupCacheFactory() {
        container = new LinkedHashMap<String, Group>();
    }

    /**
     * 如果组存在就返回，不存在就创建，保证不为null
     *
     * @param key
     * @return
     */
    public Group group(String key, int capacity) {
        //加同步，否则可能引起线程问题
        synchronized (container) {
            Group group = container.get(key);
            if (group == null) {
                group = new Group(capacity);
                container.put(key, group);
            }
            return group;
        }
    }

    /**
     * 如果组存在就返回，不存在就创建，默认容量1000
     *
     * @param key
     * @return
     */
    public Group group(String key) {

        return this.group(key, 1000);
    }
}
