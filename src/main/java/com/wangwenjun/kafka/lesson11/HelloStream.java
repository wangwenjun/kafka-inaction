package com.wangwenjun.kafka.lesson11;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

/***************************************
 * @author:Alex Wang
 * @Date:2018/4/29
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class HelloStream
{
    public static void main(String[] args)
    {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> stream = builder.stream(Serdes.String(), Serdes.String(), "stream-in");
        KStream<String, String> stringKStream = stream.mapValues(String::toUpperCase);
        stringKStream.to(Serdes.String(), Serdes.String(), "stream-out");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();
    }
}