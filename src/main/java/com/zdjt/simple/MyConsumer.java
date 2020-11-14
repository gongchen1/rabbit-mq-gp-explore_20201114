package com.zdjt.simple;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
/*
 *@Title 学习RabbitMQ---基于java api
 *@Description 消息消费者
 *Author Administrator
 *@Date 2020/11/1 0001  上午 11:19
 *Version 1.0
*/

public class MyConsumer {

    private static final String EXCHANGE_NAME ="SIMPLE_EXCHANGE" ;
    private static final String QUEUE_NAME = "SIMPLE_QUEUE";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory  factory = new ConnectionFactory();
        //连接ip
        factory.setHost("127.0.0.1");
        //默认监听端口
        factory.setPort(5672);
        //设置虚拟机
        factory.setVirtualHost("/");
        //设置访问的用户名和密码
        factory.setUsername("guest");
        factory.setPassword("guest");

        //建立连接
        Connection connection = factory.newConnection();
        //创建消息通道
        Channel channel= connection.createChannel();

        //声明交换机
        //String exchange, String type, boolean durable, boolean autoDelete,Map<String, Object> arguments
        //String exchange:the name of the exchange
        //String  type: the exchange type
        //boolean durable： true if we are declaring a durable exchange (the exchange will survive a server restart)
        //boolean autoDelete： true if the server should delete the exchange when it is no longer in use
        //Map arguments： other properties (construction arguments) for the exchange
        //* @return a declaration-confirm method to indicate the exchange was successfully declared
        channel.exchangeDeclare(EXCHANGE_NAME,"direct",
                false,false,null);
        //声明队列
       /* @param queue the name of the queue
        * @param durable true if we are declaring a durable queue (the queue will survive a server restart)
        * @param exclusive true if we are declaring an exclusive queue (restricted to this connection)
        * @param autoDelete true if we are declaring an autodelete queue (server will delete it when no longer in use)
        * @param arguments other properties (construction arguments) for the queue
        **/
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        System.out.println("waiting for messagre:");
        /**
         * 绑定一个队列到交换机上
         * @see com.rabbitmq.client.AMQP.Queue.Bind
         * @see com.rabbitmq.client.AMQP.Queue.BindOk
         * @param queue the name of the queue
         * @param exchange the name of the exchange
         * @param routingKey the routing key to use for the binding
         */
        channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,"gupao.best");
        //创建消费者
        Consumer  consumer = new DefaultConsumer(channel){
            /**
             * Called when a <code><b>basic.deliver</b></code> is received for this consumer.
             * @param consumerTag the <i>consumer tag</i> associated with the consumer
             * @param envelope packaging data for the message
             * @param properties content header data for the message
             * @param body the message body (opaque, client-specific byte array)
             * @throws IOException if the consumer encounters an I/O error while processing the message
             * @see Envelope
             */
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);
                String msg = new String(body,"utf-8");
                System.out.println("Received message : '" + msg + "'");
                System.out.println("consumerTag : " + consumerTag );
                System.out.println("deliveryTag : " + envelope.getDeliveryTag() );
                System.out.println("deliveryTag : " + properties.toString() );
            }
        };
        //获取消息
        //String queue, boolean autoAck, Consumer callback
        // * @param queue the name of the queue
        //* @param autoAck true if the server should consider messages acknowledged once delivered; false if the server should expect
        // * explicit acknowledgements
        //* @param callback an interface to the consumer object
       channel.basicConsume(QUEUE_NAME,true,consumer);




    }
}
