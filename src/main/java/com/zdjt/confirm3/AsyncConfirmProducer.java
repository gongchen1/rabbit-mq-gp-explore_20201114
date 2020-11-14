package com.zdjt.confirm3;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zdjt.util.ResourceUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/*
 *@Title  消息生产者，用来测试异步confirm模式
 * 参考文章：https://www.cnblogs.com/vipstone/p/9350075.html
 * @Description
 *@Author bob.chen
 *@Date 2020/11/14 0014  下午 8:18
 *Version 1.0
*/
public class AsyncConfirmProducer {
    private static final  String QUEUE_NAME="QUEUE_gp_async_confirm";

    public static void main(String[] args) throws  Exception {
        ConnectionFactory factory = new ConnectionFactory();
        //通过uri字符串设置连接RabbitMQ服务器的属性（主机，端口，用户名、密码等等）
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));
        //创建连接
        Connection connection = factory.newConnection();
        //创建消息通道
        Channel  channel = connection.createChannel();
        String msg = "Hello world, Rabbit MQ, Async Confirm";
        //声明队列（默认交换机:AMQP Default, Direct）
        //String queue, boolean durable, boolean exclusive, boolean autoDelete,Map<String, Object> arguments
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        //用来维护--未确认消息的deliveryTag
        SortedSet<Long> confirmSet = Collections.synchronizedSortedSet(new TreeSet<Long>());

        // 这里不会打印所有响应的ACK；ACK可能有多个，有可能一次确认多条，也有可能一次确认一条
        //异步监听确认和未确认的消息（如果要重复运行，先停掉之前的生产者，清空队列）
        channel.addConfirmListener(new ConfirmListener(){
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                // 如果true表示批量执行了deliveryTag这个值以前（小于deliveryTag的）的所有消息，如果为false的话表示单条确认
                System.out.println(String.format("broker已确认消息，标识：%d,多条消息：%b",deliveryTag,multiple));
                if (multiple) {
                    // headSet表示后面参数之前的所有元素，全部删除
                    confirmSet.headSet(deliveryTag + 1L).clear();
                } else {
                    // 只移除一个元素
                    confirmSet.remove(deliveryTag);
                }
                System.out.println("未确认的消息:"+confirmSet);

            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("Broker未确认消息，标识：" + deliveryTag);
                if(multiple){
                    // headSet表示后面参数之前的所有元素，全部删除
                    confirmSet.headSet(deliveryTag+1L).clear();
                }else {
                    confirmSet.remove(deliveryTag);
                }
                // 这里添加重发的方法
            }
        });

        //开启发送方确认模式
        channel.confirmSelect();
        for (int i = 0; i <5 ; i++) {
            long nextSeqNo = channel.getNextPublishSeqNo();
            //String exchange, String routingKey, BasicProperties props, byte[] body
            channel.basicPublish("",QUEUE_NAME,null,(msg+"_"+i).getBytes());
            confirmSet.add(nextSeqNo);
        }
        System.out.println("所有消息:"+confirmSet);
        // 这里注释掉的原因是如果先关闭了，可能收不到后面的ACK
        //channel.close();
        //conn.close();
    }
}
