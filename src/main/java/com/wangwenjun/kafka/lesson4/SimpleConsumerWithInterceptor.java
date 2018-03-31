package com.wangwenjun.kafka.lesson4;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/***************************************
 * @author:Alex Wang
 * @Date:2018/2/17
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class SimpleConsumerWithInterceptor
{
    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumerWithInterceptor.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test14"));

        for (; ; )
        {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record ->
            {
                //biz handler.
                LOG.info("P:{},V:{},OFS:{}", record.partition(), record.value(), record.offset());
            });
        }


    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "G5");
        props.put("client.id", "demo-consumer-client");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "10000");
//        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.wangwenjun.kafka.lesson4.MyConsumerInterceptor");
        return props;
    }
}