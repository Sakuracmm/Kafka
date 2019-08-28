package com.ll.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author LL
 * @date 2019/8/28
 * @description 计数拦截器
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {

    private int successCount = 0;
    private int failCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null ){
            successCount ++;
        }else{
            failCount ++;
        }
    }

    @Override
    public void close() {
        System.out.println("发送成功次数：" + successCount);
        System.out.println("发送失败次数：" + failCount);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
