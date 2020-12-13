package com.zdjt.ttl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zdjt.util.ResourceUtil;

import java.util.HashMap;
import java.util.Map;

/*
 *@Title 消息生产者，通过TTL学习死信队列。
 * 通过设置队列属性x-message-ttl，让队列中的消息统一过期 这种方式和单独设置消息过期对比，看看运行结果，
 * 以哪种过期作为标准。
 *@Description TODO
 *@Author bob.chen
 *@Date 2020/11/17 0017  下午 11:25
 *Version 1.0
*/
public class TTLProducer {
   private static final  String QUEUE = "TEST_TTL_QUEUE";
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        // 建立连接
        Connection conn = factory.newConnection();
        // 创建消息通道
        Channel channel = conn.createChannel();

        //通过设置队列属性来x-message-ttl设置队列中所有消息的统一过期时间
        Map<String,Object> map = new HashMap<>();
        map.put("x-message-ttl",6000);

        // 声明队列（默认交换机AMQP default，Direct）
        //String queue, boolean durable, boolean exclusive, boolean autoDelete,Map<String, Object> arguments
        channel.queueDeclare(QUEUE,false,false,false,map);


        // 对每条消息设置过期时间
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .deliveryMode(2)//持久化消息
                .contentEncoding("UTF-8")//设置消息编码格式
                .expiration("1000")//TTL
                .build();

        String msg ="Hello world, Rabbit MQ, DLX MSG";

        //发送消息.
        //String exchange, String routingKey, BasicProperties props, byte[] body
        channel.basicPublish("",QUEUE,props,msg.getBytes());


    }
}
