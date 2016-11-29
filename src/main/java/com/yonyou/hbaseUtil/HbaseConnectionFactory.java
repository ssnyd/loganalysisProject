package com.yonyou.hbaseUtil;

import com.yonyou.conf.ConfigurationManager;
import com.yonyou.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Created by ChenXiaoLei on 2016/11/8.
 */
public class HbaseConnectionFactory {
    static HbaseConnectionFactory singleton;
    static String tableName;
    static String columnFamily;
    static HTable hTable;
    static Object locker = new Object();
    private HbaseConnectionFactory(String tableName, String columnFamily) {
        HbaseConnectionFactory.tableName = tableName;
        HbaseConnectionFactory.columnFamily = columnFamily;
    }

    public static HTable gethTable(String tableName,String columnFamily){
        getInstance(tableName,columnFamily);
        return hTable;
    }

    public static HbaseConnectionFactory getInstance(String tableName,String columnFamily) {
        if (singleton == null) {
            synchronized (locker) {
                if (singleton == null) {
                    singleton = new HbaseConnectionFactory(tableName, columnFamily);
                    initialize();
                }
            }
        }
        return singleton;
    }
    private static void initialize() {
        if (hTable == null) {
            synchronized (locker) {
                if (hTable == null) {
                    Configuration hConfig = HBaseConfiguration.create();
                    hConfig.set("hbase.zookeeper.property.clientPort", ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_CLIENTPORT));
                    hConfig.set("hbase.zookeeper.quorum", ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_QUORUM));
                    hConfig.set("hbase.defaults.for.version.skip", "true");
                    try {
                        Connection connection = ConnectionFactory.createConnection(hConfig);
                        hTable = (HTable)connection.getTable(TableName.valueOf(tableName));
                        hTable.setAutoFlush(false,false);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
