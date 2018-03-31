package com.wangwenjun.kafka.lesson4;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/***************************************
 * @author:Alex Wang
 * @Date:2018/4/1
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class MyProducerInterceptor implements ProducerInterceptor<String, String>
{
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record)
    {
        return new ProducerRecord<>(record.topic(), record.partition(),
                record.timestamp(), record.key(),
                record.value().toUpperCase(), record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception)
    {

    }

    @Override
    public void close()
    {

    }

    @Override
    public void configure(Map<String, ?> configs)
    {

    }
}
