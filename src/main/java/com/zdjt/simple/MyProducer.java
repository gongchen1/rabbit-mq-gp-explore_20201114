package com.zdjt.simple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/*
 *@Title 消息生产者
 *@Description 学习RabbitMQ---基于java api
 *Author bob.chen
 *@Date 2020/11/1 0001  下午 6:04
 *Version 1.0
*/

public class MyProducer {
    private final static String EXCHANGE_NAME = "SIMPLE_EXCHANGE";
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory  factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        // 虚拟机
        factory.setVirtualHost("/");
        //设置访问用户和密码
        factory.setUsername("guest");
        factory.setPassword("guest");

        //创建连接
       Connection connection = factory.newConnection();
       //创建发送消息的通道
       Channel channel = connection.createChannel();
       String message ="Hello world, RabbitMQ!";
       //发送消息
        //String exchange, String routingKey, BasicProperties props, byte[] body
       channel.basicPublish(EXCHANGE_NAME,"gupao.best",null, message.getBytes());
       System.out.println("生产者已发送消息");

    }
}
