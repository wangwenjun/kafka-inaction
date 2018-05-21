package com.wangwenjun.kafka.lesson11;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

/***************************************
 * @author:Alex Wang
 * @Date:2018/4/29
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class WordCountStream
{
    public static void main(String[] args)
    {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wc-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> stream = builder.stream(Serdes.String(), Serdes.String(), "stream-wc-in");
        KStream<String, String> wordsStream = stream.flatMapValues(v -> Arrays.asList(v.toUpperCase().split("\\W+")));
        KTable<String, Long> counting = wordsStream.groupBy((k, v) -> v).count("counting");

        counting.to(Serdes.String(), Serdes.Long(), "stream-wc-out");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
