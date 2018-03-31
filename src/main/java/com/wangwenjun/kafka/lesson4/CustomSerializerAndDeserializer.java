package com.wangwenjun.kafka.lesson4;

import com.wangwenjun.kafka.lesson4.internal.User;
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
public class CustomSerializerAndDeserializer
{
    private static final Logger LOG = LoggerFactory.getLogger(CustomSerializerAndDeserializer.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test13"));


        for (; ; )
        {
            ConsumerRecords<String, User> records = consumer.poll(100);
            records.forEach(record ->
            {
                //biz handler.
                LOG.info("key:{},value:{}", record.key(), record.value());
            });
        }
    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.wangwenjun.kafka.lesson4.internal.UserDeserializer");
        props.put("group.id", "g1");
        props.put("client.id", "customer-ser");
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}