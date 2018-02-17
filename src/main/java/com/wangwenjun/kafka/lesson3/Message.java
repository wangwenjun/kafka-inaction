package com.wangwenjun.kafka.lesson3;

/***************************************
 * @author:Alex Wang
 * @Date:2018/2/16
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/
public class Message
{
    private final int id;
    private final String name;

    public Message(int id, String name)
    {
        this.id = id;
        this.name = name;
    }

    public int getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }
}