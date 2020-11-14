package com.zdjt.ack;
/*
 *@Title 消息消费者，用于测试消费者手工应答和重回队列
 *@Description TODO
 *Author Administrator
 *@Date 2020/11/7 0007  下午 7:18
 *Version 1.0
*/

import com.rabbitmq.client.*;
import com.zdjt.util.ResourceUtil;

import java.io.IOException;

public class AckConsumer {
    private static final String QUEUE_NAME ="TEST_ACK_QUEUE" ;

    public static void main(String[] args) throws  Exception {
        ConnectionFactory   factory = new ConnectionFactory();
        //uri字符串的形式：amqp://guest:guest@127.0.0.1:5672。通过把这个uri放在xx.properties配置中，如resources下的config.prperties中，然后在项目中获取
        //可以在AMQP URI中设置字段的方法(解析传入的uri字符串来给host, port, username, password and virtual host设置值)
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));
        //建立连接
        Connection connection = factory.newConnection();
        //创建消息通道
        final Channel channel = connection.createChannel();
        //声明交换机（如果没有指定交换机，那么使用的是默认的交换机：默认交换机AMQP default，Direct）
        //声明队列
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        System.out.println("waiting for message....");

        //创建消费者，接收消息
        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleConsumeOk(String consumerTag) {
                super.handleConsumeOk(consumerTag);
            }

            @Override
            public void handleCancelOk(String consumerTag) {
                super.handleCancelOk(consumerTag);
            }

            @Override
            public void handleCancel(String consumerTag) throws IOException {
                super.handleCancel(consumerTag);
            }

            @Override
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                super.handleShutdownSignal(consumerTag, sig);
            }

            @Override
            public void handleRecoverOk(String consumerTag) {
                super.handleRecoverOk(consumerTag);
            }

            //处理发送（生产者发送消息，消费端接收消息）当该消费者接收到一个basic.deliver，那么这个方法会被调用
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
               // super.handleDelivery(consumerTag, envelope, properties, body);
                System.out.println("consumerTag:"+consumerTag);
                String  msg = new String(body,"UTF-8");
                System.out.println("Received message : "+msg);
                System.out.println("envelope:"+envelope);
                if(msg.contains("拒绝")){
                    //拒绝消息
                    //deliveryTag：传送标记
                    //requeue: true,重新入队；false:被丢弃或者进入死信队列
                    // TODO 如果只有这一个消费者，requeue 为true 的时候会造成消息重复消费
                    System.out.println("进入处理拒绝消息。。。");
                    channel.basicReject(envelope.getDeliveryTag(),false);
                }else if(msg.contains("异常")){
                    //批量拒绝，拒绝一个或者多个消息
                    //multiple: true,拒绝所有的消息，false,仅拒绝提供标记的消息
                    //requeue: true,重新入队；false:被丢弃或者进入死信队列
                    // (Note: TODO） 如果只有这一个消费者，requeue 为true 的时候会造成消息重复消费
                    System.out.println("进入处理批量拒绝。。。");
                    channel.basicNack(envelope.getDeliveryTag(),true,false);
                }else{
                    // 手工应答
                    // 如果不应答，队列中的消息会一直存在，重新连接的时候会重复消费
                    System.out.println("进入处理手工应答。。。");
                    channel.basicAck(envelope.getDeliveryTag(),true);
                }
            }

            @Override
            public Channel getChannel() {
                return super.getChannel();
            }

            @Override
            public String getConsumerTag() {
                return super.getConsumerTag();
            }
        };

        // 开始获取消息，注意这里开启了手工应答（auotoAck=false）
        // String queue, boolean autoAck, Consumer callback


        /**
         * 我的注释：
         * 启动一个非本地、非独占的使用者，这个消费会有一个服务器生成的consumerTag
         * @param queue：队列的名称
         * @param autoAck：
         *          -true; 如果传递消息后,服务器应该考虑已确认的消息，那我们就设置true;
         *          -false:如果服务器期待显示确认，那我们就设置false.
         * @param callback  消费者对象（回调入口）
         * @retunn 返回一个由服务器生成的consumerTag
         */
       String  consumerTag = channel.basicConsume(QUEUE_NAME, false, consumer);
       System.out.println("consumerTag:"+consumerTag);

    }
}
