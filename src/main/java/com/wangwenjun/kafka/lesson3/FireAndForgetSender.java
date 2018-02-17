package com.wangwenjun.kafka.lesson3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

/***************************************
 * @author:Alex Wang
 * @Date:2018/2/15
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class FireAndForgetSender
{

    private final static Logger LOGGER = LoggerFactory.getLogger(FireAndForgetSender.class);

    public static void main(String[] args)
    {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        IntStream.range(0, 10).forEach(i ->
        {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("fire_and_forget_sender", String.valueOf(i), "hello " + i);
            producer.send(record);
            LOGGER.info("The message is send done and the key is {}", i);
        });
        producer.flush();
        producer.close();
    }

    private static Properties initProps()
    {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
