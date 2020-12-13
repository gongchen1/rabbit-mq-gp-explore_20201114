package com.zdjt.dlx;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zdjt.util.ResourceUtil;

/*
 *@Title 消息生产者：通过TTL测试死信队列
 *@Description TODO
 *@Author bob.chen
 *@Date 2020/11/15 0015  上午 11:21
 *Version 1.0
*/
public class DlxProducer {
    public static void main(String[] args) throws  Exception{
        //设置连接Rabbitmq服务器的属性（host,port,用户名和密码等等）
        ConnectionFactory  factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        //建立连接
        Connection conn=factory.newConnection();
        //创建消息传送通道
        Channel channel = conn.createChannel();
        //消息
        String msg ="Hello World, RabbitMQ,Dead Line X ,this is  a msg！";
        //设置基本属性，消息10s后过期
        AMQP.BasicProperties   properties = new AMQP.BasicProperties().builder()
                //持久化消息
                .deliveryMode(2)
                .contentEncoding("UTF-8")
                //TTL，消息过期时间
                .expiration("10000")
                .build();
        // 发送消息
        //String exchange, String routingKey, AMQP.BasicProperties props, byte[] body
        channel.basicPublish("","TEST_DLX_QUEUE",properties,msg.getBytes());

        channel.close();
        conn.close();
    }

}
