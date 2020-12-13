package com.zdjt.limit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zdjt.util.ResourceUtil;

import java.util.Map;

/*
 *@Title 消息生产者，用于测试消费者限流：prefetch out
 *@Description
 *@Author bob.chen
 *@Date 2020/11/16 0016  下午 9:58
 *Version 1.0
*/
public class LimitProducer {
    private final static String QUEUE_NAME = "TEST_LIMIT_QUEUE";
    public static void main(String[] args) throws  Exception{
        //设置连接RabbitMQ服务器的属性
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        //建立连接
        Connection conn=factory.newConnection();
        //创建消息传送通道
        Channel channel = conn.createChannel();
        //消息
        String msg ="a limit message";

         // 声明队列（默认交换机AMQP default，Direct）
        //String queue, boolean durable, boolean exclusive, boolean autoDelete,Map<String, Object> arguments
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        for (int i = 0; i < 100 ; i++) {
            //发送消息（采用默认的交换机时《交换机为空字符串时>，路由键就是队列名）
            //String exchange, String routingKey, BasicProperties props, byte[] body
            channel.basicPublish("",QUEUE_NAME,null,(msg+i).getBytes());
        }

        channel.close();
        conn.close();
    }
}
