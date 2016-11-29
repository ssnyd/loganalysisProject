package com.yonyou.constant;

/**
 * 常量接口
 * Created by ChenXiaoLei on 2016/11/08.
 *
 */
public interface Constants {

    /**
     * 项目配置相关的常量
     */
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    String JDBC_URL_PROD = "jdbc.url.prod";
    String JDBC_USER_PROD = "jdbc.user.prod";
    String JDBC_PASSWORD_PROD = "jdbc.password.prod";
    String SPARK_LOCAL = "spark.local";
    String ESN_API_SECRET = "esn_api_secret";
    String ESN_API_HOST = "esn_api_host";
    String ESN_API_APP = "esn_api_app";

    String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
    String HBASE_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.clientPort";
    String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    String KAFKA_TOPICS = "kafka.topics";
    String STREAMING_CHECKPOINT = "streaming_checkpoint";
    String ESN_API_ADDR = "esn_api_addr";
    //--------------------------------------------------
    String ESNSTREAMING2HBASE = "ESNStreaming2Hbase";
    String ESNSTREAMING2HBASE_TIME = "ESNStreaming2Hbase.time";
    String ESNSTREAMING2HBASE_TOPIC = "ESNStreaming2Hbase.topic";
    String ESNSTREAMING2HBASE_OFFSET_NUM = "ESNStreaming2Hbase.offset.num";
    String ESN_GROUPID = "ESN.groupid";
    String ZOOKEEPER_LIST = "zookeeper.list";
}
