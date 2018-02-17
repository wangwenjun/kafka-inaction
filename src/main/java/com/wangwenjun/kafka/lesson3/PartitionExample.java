package com.wangwenjun.kafka.lesson3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/***************************************
 * @author:Alex Wang
 * @Date:2018/2/16
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class PartitionExample
{

    private final static Logger LOGGER = LoggerFactory.getLogger(PartitionExample.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException
    {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        /*Future<RecordMetadata> future = producer.send(record);
        RecordMetadata recordMetadata = future.get();
        LOGGER.info("{}", recordMetadata.partition());

        record = new ProducerRecord<>("test_p", "hello", "hello");
        future = producer.send(record);
        recordMetadata = future.get();
        LOGGER.info("{}", recordMetadata.partition());*/

        ProducerRecord<String, String> record = new ProducerRecord<>("test_p", "XYZ", "hello");
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata recordMetadata = future.get();
        LOGGER.info("{}", recordMetadata.partition());
        producer.flush();
        producer.close();
    }

    private static Properties initProps()
    {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.wangwenjun.kafka.lesson3.MyPartitioner");
        return props;
    }
}
