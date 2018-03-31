package com.wangwenjun.kafka.lesson4.internal;

/***************************************
 * @author:Alex Wang
 * @Date:2018/4/1
 * QQ: 532500648
 * QQ群:463962286
 ***************************************/

/**
 * 4 byte
 * [4][4][10][4][20]
 *
 *
 * [4][4][10][4][20]
 *
 */
public class User
{

    private int id;

    private String name;

    private String address;

    public User(int id, String name, String address)
    {
        this.id = id;
        this.name = name;
        this.address = address;
    }

    public int getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getAddress()
    {
        return address;
    }

    @Override
    public String toString()
    {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
}
