package com.wangwenjun.kafka.lesson10;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class ConsumerGroupConsume
{
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupConsume.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("alex-3"));

        for (; ; )
        {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record ->
            {
                //business process
                LOG.info("Partition:{},offset:{},key:{},value:{}", record.partition(), record.offset(),
                        record.key(), record.value());
            });
        }
    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "Alex-Group-4");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
