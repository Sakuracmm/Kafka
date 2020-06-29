package com.ll.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;

/**
 * @author LL
 * @date 2019/8/27
 * @description kafka 生产者
 */
public class CustomerProducerWithCallback {

    public static void main(String[] args) {
        //配置信息
        Properties props = new Properties();
        //kafka集群
        props.put("bootstrap.servers", "weekend1:9092,weekend2:9092,weekend3:9092");
        //应答机制
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        props.put("retries", 0);
        //批量大小
        props.put("batch.size", 16384);
        //提交延时
        props.put("linger.ms", 1);
        //producer总共能缓存的数据大小
        props.put("buffer.memory", 33554432);
        //数据key序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //数据value序列化类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            //循环发送数据
            producer.send(new ProducerRecord<String, String>("first", String.valueOf("Alice" + i)),
                    (metadata, exception) -> {
                        if (exception == null) {
                            System.out.println("发送成功！");
//                            System.out.println("偏移量：" + metadata.offset() + "\n +" +
//                                    "分区：" + metadata.partition() + "\n" +
//                                    "接收主题：" + metadata.topic() + "\n" +
//                                    "时间戳：" + metadata.timestamp() + "\n" +
//                                    "key序列化大小：" + metadata.serializedKeySize() + "\n" +
//                                    "value序列化大小：" + metadata.serializedValueSize());
                            System.out.println("偏移量： " + metadata.offset() + "所在分区：" + metadata.partition());
                        } else {
                            System.out.println("发送失败！");
                        }
                    });
        }
        //关闭资源
        producer.close();
    }
}
