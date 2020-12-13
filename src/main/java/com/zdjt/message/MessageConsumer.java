package com.zdjt.message;

import com.rabbitmq.client.*;
import com.zdjt.util.ResourceUtil;

import java.io.IOException;
import java.util.Map;

/*
 *@Title
 *@Description 消息消费者，学习消息的其他属性。可以通过Properteis类获取生产者发送的消息相关的其他属性。详见
 * handleDelivery(xxxx)
 *@Author bob.chen
 *@Date 2020/11/16 0016  下午 10:59
 *Version 1.0
*/
public class MessageConsumer {
    private final static String QUEUE_NAME = "ORIGIN_QUEUE";
    public static void main(String[] args) throws Exception {

        //设置连接RabbitMQ服务器的属性
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        // 建立连接
        Connection conn = factory.newConnection();
        // 创建消息通道
        Channel channel = conn.createChannel();

        //声明队列（默认交换机AMQP default，Direct）
        //String queue, boolean durable, boolean exclusive, boolean autoDelete,Map<String, Object> arguments
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        System.out.println(" Waiting for message....");

        //创建消费者
        Consumer  consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body,"UTF-8");
                System.out.println("Received message : '" + msg + "' " );
                //通过properties获取生产者传送的其他属性
                System.out.println("messageId:"+ properties.getMessageId());
                System.out.println(properties.getHeaders().get("name")+"---"+properties.getHeaders().get("Level"));
            }
        };
        //开始获取消息
        //String queue, boolean autoAck, Consumer callback
        channel.basicConsume(QUEUE_NAME,false,consumer);
    }
}
