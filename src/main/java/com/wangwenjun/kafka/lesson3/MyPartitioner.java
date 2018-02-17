package com.wangwenjun.kafka.lesson3;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/***************************************
 * @author:Alex Wang
 * @Date:2018/2/16
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class MyPartitioner implements Partitioner
{

    private final String LOGIN = "LOGIN";
    private final String LOGOFF = "LOGOFF";
    private final String ORDER = "ORDER";

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster)
    {
        if (keyBytes == null || keyBytes.length == 0)
        {
            throw new IllegalArgumentException("The key is required for BIZ.");
        }

        switch (key.toString().toUpperCase())
        {
            case LOGIN:
                return 0;
            case LOGOFF:
                return 1;
            case ORDER:
                return 2;
            default:
                throw new IllegalArgumentException("The key is invalid.");
        }
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