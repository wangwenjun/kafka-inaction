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
 * @Date:2018/2/15
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class ProducerPerfWithAck
{
    private final static Logger LOGGER = LoggerFactory.getLogger(ProducerPerfWithAck.class);

    public static void main(String[] args)
    {
        Properties properties = initProps();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        long start = System.currentTimeMillis();
        IntStream.range(0, 20000).forEach(i ->
        {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("ack", String.valueOf(i), "hello " + i);
            Future<RecordMetadata> future = producer.send(record);
            try
            {
                RecordMetadata metaData = future.get();
                LOGGER.info("The message is send done and the key is {},offset {}", i, metaData.offset());

            } catch (InterruptedException e)
            {
                e.printStackTrace();
            } catch (ExecutionException e)
            {
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
        long current = System.currentTimeMillis();
        LOGGER.info("total spend {} ms", (current - start));
    }

    /**
     * acks=0  39874 ms
     * acks=1  116318 ms
     * acks=all 143375ms
     * acks=all+gzip 151835ms
     * acks=all+gzip 151835ms
     * acks=all+gzip 140043ms
     * @return
     */
    private static Properties initProps()
    {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("compression.type", "snappy");
        props.put("acks", "all");
        return props;
    }
}
