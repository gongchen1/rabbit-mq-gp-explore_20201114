package com.zdjt.rpc;

import com.rabbitmq.client.*;
import com.zdjt.util.ResourceUtil;

import java.io.IOException;
import java.util.Map;

/*
 *@Title RPC服务端。接收客户端的服务并将响应返回给客户端
 *@Description TODO
 *@Author bob.chen
 *@Date 2020/11/17 0017  下午 10:40
 *Version 1.0
*/
public class RPCServer {
    private final static String REQUEST_QUEUE_NAME="RPC_REQUEST";

    public static void main(String[] args) throws  Exception {
        //设置连接RabbitMQ服务器的属性
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        // 建立连接
        Connection conn = factory.newConnection();
        // 创建消息通道
        Channel channel = conn.createChannel();

        //String queue, boolean durable, boolean exclusive, boolean autoDelete,Map<String, Object> arguments
        channel.queueDeclare(REQUEST_QUEUE_NAME,false,false,false,null);
        System.out.println("等待消息进来。。。。");
        //设置prefetch值，一次处理一条数据
        channel.basicQos(1);


        //创建消费者，处理消息
        Consumer  consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                AMQP.BasicProperties replyProperties = new AMQP.BasicProperties.Builder()
                        .correlationId(properties.getCorrelationId())
                        .build();
                //获取客户端指定的回调队列名
                String replayQueue = properties.getReplyTo();
                //返回获取消息的平方
                String message = new String(body,"UTF-8");
                // 计算平方
                Double mSquare =  Math.pow(Integer.parseInt(message),2);
                String repMsg = String.valueOf(mSquare);

                // 把结果发送到回复队列
                //String exchange, String routingKey, BasicProperties props, byte[] body
                channel.basicPublish("",replayQueue,replyProperties,repMsg.getBytes());
                //手动恢复消息
                channel.basicAck(envelope.getDeliveryTag(),false);

            }
        };
        //获取消息
        //String queue, boolean autoAck, Consumer callback
        channel.basicConsume(REQUEST_QUEUE_NAME,true,consumer);
    }
}
