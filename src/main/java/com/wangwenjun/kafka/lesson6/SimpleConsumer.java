package com.wangwenjun.kafka.lesson6;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

/***************************************
 * @author:Alex Wang
 * @Date:2018/2/17
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class SimpleConsumer
{
    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test6"));

        for (; ; )
        {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record ->
            {
                //biz handler.
                LOG.info("offset:{}", record.offset());
                LOG.info("value:{}", record.value());
                LOG.info("key:{}", record.key());
            });
        }
    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test_group7");
        props.put("client.id", "lesson-reliable");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "10000");
        return props;
    }
}