package com.zdjt.message;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zdjt.util.ResourceUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/*
 *@Title 消息生产者，学习消息的其他属性。将消息的其他属性通过Properties实例传给消费者
 *@Description TODO
 *@Author bob.chen
 *@Date 2020/11/16 0016  下午 10:59
 *Version 1.0
*/
public class MesssageProducer {
    private final static String QUEUE_NAME = "ORIGIN_QUEUE";
    public static void main(String[] args)  throws  Exception {
        //通过uri字符串设置连接RabbitMQ服务器的必要属性（主机，端口，用户名和密码，协议等）
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        // 建立连接
        Connection conn = factory.newConnection();
        // 创建消息通道
        Channel channel = conn.createChannel();

        Map<String,Object> headers = new HashMap<String,Object>();
        headers.put("name","hello");
        headers.put("level","TOP");
        AMQP.BasicProperties  properties = new AMQP.BasicProperties().builder()
                .deliveryMode(2)//2，代表着持久化
                .contentEncoding("UTF-8")//编码
                .expiration("100000")//TTL,过期时间
                .headers(headers)
                .priority(5)// 优先级，默认为5，配合队列的 x-max-priority 属性使用
                .messageId(String.valueOf(UUID.randomUUID()))
                .build();
          String msg ="Hello,pi";

        //声明队列（默认交换机AMQP default，Direct）
        //String queue, boolean durable, boolean exclusive, boolean autoDelete,  Map<String, Object> arguments
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);


        //发送消息
        //String exchange, String routingKey, BasicProperties props, byte[] body
        channel.basicPublish("",QUEUE_NAME,properties,msg.getBytes());

    }

}
