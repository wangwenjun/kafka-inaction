package com.wangwenjun.kafka.lesson4;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/***************************************
 * @author:Alex Wang
 * @Date:2018/2/17
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class RebalanceConsumer
{
    private static final Logger LOG = LoggerFactory.getLogger(RebalanceConsumer.class);


    public static void main(String[] args)
    {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(loadProp());
        consumer.subscribe(Collections.singletonList("test16"), new ConsumerRebalanceListener()
        {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions)
            {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions)
            {

            }
        });

        for (; ; )
        {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.forEach(record ->
            {
                //business process
                LOG.info("Partition:{},offset:{},key:{},value:{}", record.partition(), record.offset(),
                        record.key(), record.value());
                try
                {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            });
            consumer.commitSync();
        }
    }

    private static Properties loadProp()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "G16");
        props.put("client.id", "C3");
        props.put("auto.offset.reset", "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        props.put("auto.commit.interval.ms", "10000");
        return props;
    }

    private static class MyConsumerRebalanceListener implements ConsumerRebalanceListener
    {

        private final KafkaConsumer<String, String> consumer;

        private MyConsumerRebalanceListener(KafkaConsumer<String, String> consumer)
        {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions)
        {
            /*for (TopicPartition partition : partitions)
            {
                long nextOffset = consumer.position(partition);
                //
                //
                //partition->nextOffset  store
                //
            }*/

            LOG.info("onPartitionsRevoked=>{}", partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions)
        {
            /*for(TopicPartition partition: partitions)
              consumer.seek(partition, 0);*/

            LOG.info("onPartitionsAssigned=>{}", partitions);
        }
    }
}