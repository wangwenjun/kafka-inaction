package com.wangwenjun.kafka.lesson4.internal;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/***************************************
 * @author:Alex Wang
 * @Date:2018/4/1
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class UserDeserializer implements Deserializer<User>
{
    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        //do nothing
    }

    @Override
    public User deserialize(String topic, byte[] data)
    {
        if (data == null)
            return null;

        if (data.length < 12)
            throw new SerializationException("The User data bytes length should not be less than 12.");

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int id = buffer.getInt();
        int nameLength = buffer.getInt();
        byte[] nameBytes = new byte[nameLength];
        buffer.get(nameBytes);
        String name = new String(nameBytes);

        int addrLength = buffer.getInt();
        byte[] addrBytes = new byte[addrLength];
        buffer.get(addrBytes);
        String address = new String(addrBytes);
        return new User(id, name, address);
    }

    @Override
    public void close()
    {
        //do nothing
    }
}
