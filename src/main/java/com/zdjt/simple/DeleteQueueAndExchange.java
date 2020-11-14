package com.zdjt.simple;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
/*
 *@Title 演示删除RabbitMQ上的队列和交互机（其实就是清空RabbitMQ上所有的记录）
 *@Description
 *Author Administrator
 *@Date 2020/11/3 0003  下午 11:38
 *Version 1.0
*/
public class DeleteQueueAndExchange {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(5672);
        // 虚拟机
        connectionFactory.setVirtualHost("/");
        // 用户
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        // 建立连接
        Connection conn = connectionFactory.newConnection();
        // 创建消息通道
        Channel channel = conn.createChannel();


        String[] queueNames = {"ORIGIN_QUEUE","GP_FIRST_QUEUE", "GP_FOURTH_QUEUE", "GP_SECOND_QUEUE", "GP_THIRD_QUEUE",
                "MY_FIRST_QUEUE", "MY_FOURTH_QUEUE", "MY_SECOND_QUEUE", "MY_THIRD_QUEUE", "SIMPLE_QUEUE"};


        String[] exchangeNames = {"GP_DIRECT_EXCHANGE","GP_FANOUT_EXCHANGE", "GP_TOPIC_EXCHANGE",
                "MY_DIRECT_EXCHANGE", "MY_FANOUT_EXCHANGE", "MY_TOPIC_EXCHANGE", "SIMPLE_EXCHANGE"};
        //删除队列
        for(String queueName :queueNames){
            channel.queueDelete(queueName);
        }

        //删除交换机
        for(String exchangeName :exchangeNames){
            channel.exchangeDelete(exchangeName);
        }
        channel.close();
        conn.close();
    }
}
