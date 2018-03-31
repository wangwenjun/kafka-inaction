package com.wangwenjun.kafka.lesson4;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/***************************************
 * @author:Alex Wang
 * @Date:2018/3/31
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class ConsumerAsyncCommit
{

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerAsyncCommit.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test12"));

        final AtomicInteger count = new AtomicInteger(0);

        for (; ; )
        {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record ->
            {
                //biz handler.
                LOG.info("offset:{}", record.offset());
                LOG.info("value:{}", record.value());
                LOG.info("key:{}", record.key());
//                if (count.incrementAndGet() == 1000)
//                {
//                    consumer.commitSync();
//                    count.set(0);
//                }
            });

            /**
             * can not be retry
             * non-block
             */
            consumer.commitAsync((map, e) ->
            {
                if (e != null)
                {
                    e.printStackTrace();
                } else
                {
                    System.out.println(map);
                }
            });
        }
    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test_group5");
        props.put("client.id", "demo-commit-consumer-client");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        return props;
    }
}
