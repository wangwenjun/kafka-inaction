package com.wangwenjun.kafka.lesson10;

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
 * @Date:2018/2/15
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class AclProducer
{

    private final static Logger LOGGER = LoggerFactory.getLogger(AclProducer.class);

    static
    {
        System.setProperty("java.security.auth.login.config", "C:\\Users\\wangwenjun\\IdeaProjects\\kafka-inaction\\src\\main\\resources\\Producer_jaas.conf");
    }

    public static void main(String[] args)
    {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        IntStream.range(0, 10).forEach(i ->
        {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("acl_test", String.valueOf(i), "hello " + i);
            Future<RecordMetadata> future = producer.send(record);
            try
            {
                RecordMetadata metaData = future.get();
                LOGGER.info("The message is send done and the key is {},offset {}", i, metaData.offset());
            } catch (InterruptedException | ExecutionException e)
            {
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
    }

    private static Properties initProps()
    {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9093,192.168.88.109:9093,192.168.88.110:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        return props;
    }
}
