package com.ll.stream;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.TopologyBuilder;

/**
 * @author LL
 * @date 2019/8/28
 * @description Kafka流处理
 */
public class KafkaStream {
    public static void main(String[] args) {

        //创建拓扑对象
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        //创建配置文件
        Properties properties = new Properties();
        //构建拓扑结构
        topologyBuilder.addSource("SOURCE","canlie")
                        .addProcessor("PROCESSOR",() -> new LogProcessor(){
                        },"SOURCE")
                        .addSink("SINK","first","PROCESSOR");


        properties.put("application.id","KafkaStream");
        properties.put("bootstrap.servers","weekend1:9092,weekend2:9092,weekend3:9092");

        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder, properties);

        kafkaStreams.start();
    }
}
