package com.ll.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author LL
 * @date 2019/8/28
 * @description 消息时间拦截器
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord<String,String> onSend(ProducerRecord producerRecord) {
        return new ProducerRecord<String, String>(producerRecord.topic(), (String) producerRecord.key(),System.currentTimeMillis() + "," + producerRecord.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
