package com.zdjt.ack;/*
 *@Title 消息生产者，用于测试消费者手工应答和重回队列
 *@Description TODO
 *Author Administrator
 *@Date 2020/11/8 0008  下午 11:54
 *Version 1.0
*/

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zdjt.util.ResourceUtil;

public class AckProducer {
    private final static String QUEUE_NAME = "TEST_ACK_QUEUE";
    public static void main(String[] args) throws  Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        // 建立连接
        Connection conn = factory.newConnection();
        // 创建消息通道
        Channel channel = conn.createChannel();

        String msg = "test ack message 拒绝 ";
        // 声明队列（默认交换机AMQP default，Direct）
        // String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        //发送消息:basicPublish
        for (int i=0;i<5;i++){
            //String exchange, String routingKey, BasicProperties props, byte[] body
            channel.basicPublish("",QUEUE_NAME,null,(msg+i).getBytes());
        }
        channel.close();
        conn.close();
    }
}
