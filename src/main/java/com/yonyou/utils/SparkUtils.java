package com.yonyou.utils;

import com.google.common.collect.ImmutableMap;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ChenXiaoLei on 2016/11/21.
 */
public class SparkUtils {

    public static Map<TopicAndPartition, Long> getConsumerOffsets(String zkServers,
                                                                  String groupID, String topic) {
        Map<TopicAndPartition, Long> retVals = new HashMap<TopicAndPartition, Long>();

        ObjectMapper objectMapper = new ObjectMapper();
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zkServers).connectionTimeoutMs(1000)
                .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
        curatorFramework.start();
        try {
            String nodePath = "/consumers/" + groupID + "/offsets/" + topic;
            if (curatorFramework.checkExists().forPath(nodePath) != null) {
                List<String> partitions = curatorFramework.getChildren().forPath(nodePath);
                for (String partiton : partitions) {
                    int partitionL = Integer.valueOf(partiton);
                    Long offset = objectMapper.readValue(curatorFramework.getData().forPath(nodePath + "/" + partiton), Long.class);
                    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionL);
                    retVals.put(topicAndPartition, offset);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        curatorFramework.close();
        return retVals;
    }

    public static Map<TopicAndPartition, Long> getTopicOffsets(String zkServers, String topic) {
        Map<TopicAndPartition, Long> retVals = new HashMap<TopicAndPartition, Long>();

        for (String zkServer : zkServers.split(",")) {
            SimpleConsumer simpleConsumer = new SimpleConsumer(zkServer.split(":")[0],
                    Integer.valueOf(zkServer.split(":")[1]),
                    10000,
                    1024,
                    "consumer");
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Arrays.asList(topic));
            TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

            for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
                for (PartitionMetadata part : metadata.partitionsMetadata()) {
                    Broker leader = part.leader();
                    if (leader != null) {
                        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, part.partitionId());

                        PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 10000);
                        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());
                        OffsetResponse offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest);
                        if (!offsetResponse.hasError()) {
                            long[] offsets = offsetResponse.offsets(topic, part.partitionId());
                            retVals.put(topicAndPartition, offsets[0]);
                        }
                    }
                }
            }
            simpleConsumer.close();
        }
        return retVals;
    }
}




