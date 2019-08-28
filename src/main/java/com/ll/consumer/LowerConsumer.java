package com.ll.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;


/**
 * @author LL
 * @date 2019/8/27
 * @description Kafka消费者 低级API【可以自定义查找分区，分区中offset等信息】，根据指定的topic,partition,offset来获取数据
 */
public class LowerConsumer {
    public static void main(String[] args) {

        //定义相关参数
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("weekend1");
        brokers.add("weekend2");
        brokers.add("weekend3");
        //端口号
        int port = 9092;
        //主题
        String topic = "canlie";
        //分区
        int partition = 0;
        //offset
        long offset = 13;

        LowerConsumer lowerConsumer = new LowerConsumer();
        lowerConsumer.getData(brokers,port,topic,partition,offset);
    }

    /**
     * 找到分区Leader
     *
     * @params
     * @return
     */
    private BrokerEndPoint findLeader(List<String> brokers, int port, String topic, int partition){

        for (String broker:
             brokers){
            //创建获取分区leader的消费者对象

            SimpleConsumer getLeader = new SimpleConsumer(broker, port, 1000, 1024 * 4, "getLeader");
            //创建主题元数据信息请求
            TopicMetadataRequest metadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            //获取主题元数据返回值
            TopicMetadataResponse metadataResponse = getLeader.send(metadataRequest);
            //解析元数据返回值
            List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
            //遍历主题元数据
//            topicsMetadata.forEach(topicMetadata -> {
//                        //获取一个主题下多个分区信息
//                        List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
//                        partitionsMetadata.forEach(partitionMetadata -> {
//                            if(partition == partitionMetadata.partitionId()){
//                                return partitionMetadata.leader();
//                            }
//                });
//            });
            for (TopicMetadata topicMetadata:
                 topicsMetadata) {
                //获取一个主题下多个分区信息
                List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
//                        partitionsMetadata.forEach(partitionMetadata -> {
//                            if(partition == partitionMetadata.partitionId()){
//                                return partitionMetadata.leader();
//                            }
                for (PartitionMetadata pa :
                        partitionsMetadata) {
                    if (partition == pa.partitionId()) {
                        return pa.leader();
                    }
                }
            }
        };

        return null;
    }

    /**
     * 获取数据
     *
     * @params
     * @return
     */
    private void getData(List<String> brokers, int port, String topic, int partition, long offset){

        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
        if(leader == null){
            return;
        }
        String leaderHost = leader.host();

        //获取数据的消费者对象
        SimpleConsumer getData = new SimpleConsumer(leaderHost, port, 1000, 1024 * 4, "getData");
        //创建获取数据的对象
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1024 * 4).build();
        //获取数据返回值
        FetchResponse fetch = getData.fetch(fetchRequest);
        //解析返回值
        ByteBufferMessageSet messageAndOffsets = fetch.messageSet(topic, partition);
        //遍历并打印
        messageAndOffsets.forEach( messageAndOffset -> {
            long offset1 = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(offset1 + " --- " + new String(bytes));
        });
    }

}
