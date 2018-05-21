package com.wangwenjun.kafka.lesson10;

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
public class AclConsumer
{
    private static final Logger LOG = LoggerFactory.getLogger(AclConsumer.class);

    static
    {
        System.setProperty("java.security.auth.login.config", "C:\\Users\\wangwenjun\\IdeaProjects\\kafka-inaction\\src\\main\\resources\\Consumer_jaas.conf");
    }

    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("acl_test"));
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
        props.put("bootstrap.servers", "192.168.88.108:9093,192.168.88.109:9093,192.168.88.110:9093");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "alex-group");
        props.put("client.id", "demo-consumer-client");
        props.put("auto.offset.reset", "earliest");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        return props;
    }
}