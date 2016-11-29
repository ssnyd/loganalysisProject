//package com.yonyou.hbaseUtil;
//
//import com.yonyou.conf.ConfigurationManager;
//import com.yonyou.constant.Constants;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by ChenXiaoLei on 2016/11/3.
// */
//public class HBaseCounter {
//    static HBaseCounter singleton;
//    static String tableName;
//    static String columnFamily;
//    static HTable hTable;
//    static long lastUsed;
//    static long flushInterval;
//    static CloserThread closerThread;
//    static FlushThread flushThread;
//    static HashMap<String, KeyValue> rowKeyValue =
//            new HashMap<String, KeyValue>();
//    static Object locker = new Object();
//    private HBaseCounter(String tableName, String columnFamily) {
//        HBaseCounter.tableName = tableName;
//        HBaseCounter.columnFamily = columnFamily;
//    }
//    public static HBaseCounter getInstance(String tableName,String columnFamily) {
//
//        if (singleton == null) {
//            synchronized (locker) {
//                if (singleton == null) {
//                    singleton = new HBaseCounter(tableName, columnFamily);
//                    initialize();
//                }
//            }
//        }
//        return singleton;
//    }
//    private static void initialize() {
//        if (hTable == null) {
//            synchronized (locker) {
//                if (hTable == null) {
//                    Configuration hConfig = HBaseConfiguration.create();
//                    hConfig.set("hbase.zookeeper.property.clientPort", ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_CLIENTPORT));
//                    hConfig.set("hbase.zookeeper.quorum", ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_QUORUM));
//                    hConfig.set("hbase.defaults.for.version.skip", "true");
//                    try {
//                        Connection connection = ConnectionFactory.createConnection(hConfig);
//                        hTable = (HTable)connection.getTable(TableName.valueOf(tableName));
//                        hTable.setAutoFlush(false,false);
//                        updateLastUsed();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    flushThread = new FlushThread(flushInterval);
//                    flushThread.start();
//                    closerThread = new CloserThread();
//                    closerThread.start();
//                }
//            }
//        }
//    }
//    public void add(String rowKey, String key, String value) {
//        KeyValue val = rowKeyValue.get(rowKey);
//        if (val == null) {
//            val = new KeyValue();
//            rowKeyValue.put(rowKey, val);
//        }
//        val.setKV(key, value);
//    }
//    private static void updateLastUsed() {
//        lastUsed = System.currentTimeMillis();
//    }
//    public static class CloserThread extends Thread {
//
//        boolean continueLoop = true;
//
//        @Override
//        public void run() {
//            while (continueLoop) {
//
//                if (System.currentTimeMillis() - lastUsed > 3000) {
//                    singleton.close();
//                    break;
//                }
//                try {
//                    Thread.sleep(5000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        public void stopLoop() {
//            continueLoop = false;
//        }
//    }
//    protected static void close() {
//        if (hTable != null) {
//            synchronized (locker) {
//                if (hTable != null) {
//                    if (hTable != null && System.currentTimeMillis() - lastUsed > 3000) {
//                        try {
//                            hTable.close();
//                        } catch (IOException e) {
//                            // TODO Auto-generated catch block
//                            e.printStackTrace();
//                        }
//                        hTable = null;
//                    }
//                }
//            }
//        }
//    }
//    protected static class FlushThread extends Thread {
//    long sleepTime;
//    boolean continueLoop = true;
//
//    public FlushThread(long sleepTime) {
//      this.sleepTime = sleepTime;
//    }
//    @Override
//    public void run() {
//      while (continueLoop) {
//        try {
//          flushToHBase();
//        } catch (IOException e) {
//          e.printStackTrace();
//          break;
//        }
//        try {
//          Thread.sleep(sleepTime);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//    }
//    private void flushToHBase() throws IOException {
//      synchronized (hTable) {
//            List<Put> puts = new ArrayList<Put>();
//            boolean hasColumns = false;
//            for (Map.Entry<String, KeyValue> entry : rowKeyValue.entrySet()){
//                if (!(entry == null)){
//                    Put put = new Put((String.valueOf(entry.getKey())).getBytes());
//                    put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(entry.getValue().getK()),Bytes.toBytes(entry.getValue().getV()));
//                    puts.add(put);
//                    hasColumns=true;
//                }
//            }
//          if (hasColumns) {
//              try {
//                  updateLastUsed();
//                  hTable.put(puts);
//                  hTable.flushCommits();
//              } catch (IOException e) {
//                  e.printStackTrace();
//              }
//          }
//          updateLastUsed();
//      }
//    }
//
//    public void stopLoop() {
//      continueLoop = false;
//    }
//  }
//}
////*********************************************************************************************************************
////package com.yonyou.hbaseUtil;
////
////import com.yonyou.conf.ConfigurationManager;
////import com.yonyou.constant.Constants;
////import org.apache.hadoop.conf.Configuration;
////import org.apache.hadoop.hbase.HBaseConfiguration;
////import org.apache.hadoop.hbase.TableName;
////import org.apache.hadoop.hbase.client.Connection;
////import org.apache.hadoop.hbase.client.ConnectionFactory;
////import org.apache.hadoop.hbase.client.HTable;
////import org.apache.hadoop.hbase.client.Put;
////import org.apache.hadoop.hbase.util.Bytes;
////
////import java.io.IOException;
////import java.util.ArrayList;
////import java.util.HashMap;
////import java.util.List;
////import java.util.Map;
////
/////**
//// * Created by ChenXiaoLei on 2016/11/3.
//// */
////public class HBaseCounter {
////    static HBaseCounter singleton;
////    static String tableName;
////    static String columnFamily;
////    static HTable hTable;
////    static long lastUsed;
////    static HashMap<String, KeyValue> rowKeyValue =
////      new HashMap<String, KeyValue>();
////    static Object locker = new Object();
////    private HBaseCounter(String tableName, String columnFamily) {
////        HBaseCounter.tableName = tableName;
////        HBaseCounter.columnFamily = columnFamily;
////    }
////    public static HBaseCounter getInstance(String tableName,String columnFamily) {
////
////        if (singleton == null) {
////            synchronized (locker) {
////                if (singleton == null) {
////                    singleton = new HBaseCounter(tableName, columnFamily);
////                    initialize();
////                }
////            }
////        }
////        return singleton;
////    }
////    private static void initialize() {
////        if (hTable == null) {
////            synchronized (locker) {
////                if (hTable == null) {
////                    Configuration hConfig = HBaseConfiguration.create();
////                    hConfig.set("hbase.zookeeper.property.clientPort", ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_CLIENTPORT));
////                    hConfig.set("hbase.zookeeper.quorum", ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_QUORUM));
////                    hConfig.set("hbase.defaults.for.version.skip", "true");
////                    try {
////                        Connection connection = ConnectionFactory.createConnection(hConfig);
////                        hTable = (HTable)connection.getTable(TableName.valueOf(tableName));
////                        hTable.setAutoFlush(false,false);
////                        updateLastUsed();
////                    } catch (IOException e) {
////                        e.printStackTrace();
////                    }
////                }
////            }
////        }
////    }
////    public void add(String rowKey, String key, String value) {
////        KeyValue val = rowKeyValue.get(rowKey);
////        if (val == null) {
////            val = new KeyValue();
////            rowKeyValue.put(rowKey, val);
////        }
////        val.setKV(key, value);
////        flushToHBase();
////    }
////    private static void updateLastUsed() {
////        lastUsed = System.currentTimeMillis();
////    }
////    public static class CloserThread extends Thread {
////
////        boolean continueLoop = true;
////
////        @Override
////        public void run() {
////            while (continueLoop) {
////
////                if (System.currentTimeMillis() - lastUsed > 3000) {
////                    singleton.close();
////                    break;
////                }
////                try {
////                    Thread.sleep(5000);
////                } catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
////            }
////        }
////        public void stopLoop() {
////            continueLoop = false;
////        }
////    }
////    protected static void close() {
////        if (hTable != null) {
////            synchronized (locker) {
////                if (hTable != null) {
////                    if (hTable != null && System.currentTimeMillis() - lastUsed > 3000) {
////                        try {
////                            hTable.close();
////                        } catch (IOException e) {
////                            // TODO Auto-generated catch block
////                            e.printStackTrace();
////                        }
////                        hTable = null;
////                    }
////                }
////            }
////        }
////    }
////    protected static void flushToHBase()  {
////        synchronized (hTable) {
////            List<Put> puts = new ArrayList<Put>();
////            boolean hasColumns = false;
////            for (Map.Entry<String, KeyValue> entry : rowKeyValue.entrySet()){
////                Put put = new Put((String.valueOf(entry.getKey())).getBytes());
////                put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(entry.getValue().getK()),Bytes.toBytes(entry.getValue().getV()));
////                puts.add(put);
////                hasColumns=true;
////            }
////            if (hasColumns) {
////                try {
////                    hTable.put(puts);
////                    hTable.flushCommits();
////                } catch (IOException e) {
////                    e.printStackTrace();
////                }
////            }
////        }
////    }
////}