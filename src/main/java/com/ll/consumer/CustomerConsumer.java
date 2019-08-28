package com.ll.consumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * @author LL
 * @date 2019/8/27
 * @description Kafka 消费者
 */
public class CustomerConsumer {
    public static void main(String[] args) {
        //配置信息
        Properties props = new Properties();
        //Kafka集群
        props.put("bootstrap.servers", "weekend1:9092,weekend2:9092,weekend3:9092");
        //消费者组id
        props.put("group.id", "xxx");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");        //重复消费方式一
        //是否自动提交，设置自动提交offset
        props.put("enable.auto.commit", "true");
        //提交延时
        props.put("auto.commit.interval.ms","1000");
        //KV的反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);



        //指定topic
        consumer.subscribe(Arrays.asList("first","canlie","third"));

        //重复消费方式二,从指定位置读取消息,这里已经指定了topic，所以需要将上面指定topic的代码注释掉
//        consumer.assign(Collections.singletonList(new TopicPartition("canlie",0)));
//        consumer.seek(new TopicPartition("canlie",0),2);

        while (true) {
            //获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            consumerRecords.forEach(record -> {
                System.out.println(record.topic() + " == " + record.partition() + " == " + record.value());
            });
        }
    }
}
