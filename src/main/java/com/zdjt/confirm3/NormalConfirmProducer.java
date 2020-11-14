package com.zdjt.confirm3;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zdjt.util.ResourceUtil;

/*
 *@Title TODO 消息生产者，普通确认模式
 *@Description 测试Confirm模式
 *@Author bob.chen
 *@Date 2020/11/14 0014  下午 9:04
 *Version 1.0
*/
public class NormalConfirmProducer  {
    private final static String QUEUE_NAME = "QUEUE_gp_normal_confirm";
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        // 建立连接
        Connection conn = factory.newConnection();
        // 创建消息通道
        Channel channel = conn.createChannel();

        String msg = "Hello world, Rabbit MQ ,Batch Confirm";
        // 声明队列（默认交换机AMQP default，Direct）
        // String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            //开启发布方确认模式
            channel.confirmSelect();
            //发送消息
            channel.basicPublish("1",QUEUE_NAME,null,msg.getBytes());

            //普通模式的confirm(确认机制)：发送成功一条，确认一条
            if (channel.waitForConfirms()){
                System.out.println("消息发送成功");
            }else{
                System.out.println("消息发送失败");
            }

        channel.close();
        conn.close();
    }
}
