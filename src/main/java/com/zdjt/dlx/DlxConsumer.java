package com.zdjt.dlx;

import com.rabbitmq.client.*;
import com.zdjt.util.ResourceUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/*
 *@Title 消息消费者：测试死信队列
 * （这里我们故意注释掉，之后可以放开再对比输出）由于消费的代码被注释掉了，10秒钟后，消息会从正常队列
 *  TEST_DLX_QUEUE 到达死信交换机 DLX_EXCHANGE ，然后路由到死信队列DLX_QUEUE，所以消息的流转过程是：
 *  消息进入正常queue-->消息在正常queue过期（没有被消费）--》到达死信交换机---》路由到死信队列DLX_QUEUE
 *@Description TODO
 *@Author bob.chen
 *@Date 2020/11/15 0015  下午 12:02
 *Version 1.0
*/
public class DlxConsumer {
    private static final String QUEUE_NAME="TEST_DLX_QUEUE";
    private static final String DEAD_QUEUE_NAME="DLX_QUEUE";
    private static final String DEAD_EXCHANGE_NAME="DLX_EXCHANGE";
    public static void main(String[] args) throws  Exception {
        //设置连接Rabbitmq服务器的属性（host,port,用户名和密码等等）
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        //建立连接
        Connection conn=factory.newConnection();
        //创建消息传送通道
        Channel channel = conn.createChannel();

        //设置普通队列的死信交换机（那么这么看来，可以理解死信交换机是常规队列的属性吗？）
        Map<String,Object> arguments = new HashMap<>();
        arguments.put("x-dead-letter-exchange","DLX_EXCHANGE");
        // arguments.put("x-expires","9000"); // 设置队列的TTL
        // arguments.put("x-max-length", 4); // 如果设置了队列的最大长度，超过长度时，先入队的消息会被发送到DLX

        // 声明队列（默认交换机AMQP default，Direct）
        // String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments
        channel.queueDeclare(QUEUE_NAME,false,false,false,arguments);


        //声明死信交换机
        channel.exchangeDeclare(DEAD_EXCHANGE_NAME,"topic",false,
                false,false,null);
        //声明死信队列
        channel.queueDeclare(DEAD_QUEUE_NAME,false,false,false,null);
       //这里，将死信队列和死信交换机通过路由键#绑定
        //String queue, String exchange, String routingKey
        channel.queueBind(DEAD_QUEUE_NAME,DEAD_EXCHANGE_NAME,"#");
        System.out.println(" Waiting for message....");

        //创建消费者
        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String  msg = new String(body,"UTF-8");
                System.out.println("Received message : " +msg);
            }
        };

        //获取消息（如果注释掉这行代码，意味着，生产者发出的消息在队列中无人消费，此时如果我们设置了消息过期时间
        // 那么消息就会通过死信交换机进入死信队列。所以测试消息的TTL时，可以注释掉这行代码）
        //String queue, boolean autoAck, Consumer callback
       // channel.basicConsume(QUEUE_NAME,true,consumer);

    }
}
