package com.zdjt.limit;

import com.rabbitmq.client.*;
import com.zdjt.util.ResourceUtil;

import java.io.IOException;
import java.util.Map;

/*
 *@Title 消息消费者，测试消费端限流（prefetch out）
 *@Description 通过给不同消费者设置不同的消息处理能力里prefectch out ，从而实现消费端限流。能者多劳，不能
 * 者尽量处理自己获取到的消息。
 *@Author bob.chen
 *@Date 2020/11/16 0016  下午 10:07
 *Version 1.0
*/
public class LimitConsumer {
    private final static String QUEUE_NAME = "TEST_LIMIT_QUEUE";
    public static void main(String[] args) throws  Exception{
        ConnectionFactory  factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        //建立连接
        Connection connection = factory.newConnection();
        //创建消息通道
        Channel channel = connection.createChannel();

        // 声明队列（默认交换机AMQP default，Direct,默认交换机的路由键是队列名）
        //String queue, boolean durable, boolean exclusive, boolean autoDelete,Map<String, Object> arguments
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        System.out.println("Consumer1  Waiting for message....");

        //创建消费者，并接收消息，然后回调处理
        Consumer  consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body,"UTF-8");
                System.out.println("Consumer1 Received message : '" + msg + "'" );

                //???
                channel.basicAck(envelope.getDeliveryTag(), true);
            }
        };
        //消费消息
        //String queue, boolean autoAck, Consumer callback
        //非自动确认消息的前提下，如果一定数目的消息（通过基于consume或者channel设置Qos的值）未被确认前，不进行消费新的消息。
        channel.basicQos(2);
        channel.basicConsume(QUEUE_NAME,false,consumer);




    }
}
