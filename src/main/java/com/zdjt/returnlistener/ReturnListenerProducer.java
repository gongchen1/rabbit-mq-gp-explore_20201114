package com.zdjt.returnlistener;

import com.rabbitmq.client.*;
import com.zdjt.util.ResourceUtil;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/*
 *@Title 消息生产者。当消息无法匹配到队列时，会发回给生产者
 *@Description TODO
 *@Author bob.chen
 *@Date 2020/11/18 0018  下午 10:39
 *Version 1.0
*/
public class ReturnListenerProducer {
    public static void main(String[] args) throws Exception {
        // 通过uri字符串设置连接RabbitMQ服务器的必要属性（主机，端口，用户名和密码，协议等）
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(ResourceUtil.getKey("rabbitmq.uri"));

        //创建连接
        Connection connection = factory.newConnection();

        //创建通道
        Channel channel = connection.createChannel();

        //监听器
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange,
                                     String routingKey, AMQP.BasicProperties properties,
                                     byte[] body) throws IOException {
                System.out.println("====监听器收到了无法路由，被返回的消息===========");
                System.out.println("replyCode:"+replyCode);
                System.out.println("replyText:"+replyText);
                System.out.println("properties:"+properties);
                System.out.println("body:"+new String(body));

            }
        });
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .deliveryMode(2)//设置消息持久化
                .contentEncoding("UTF-8")//设置消息内容表明格式
                .build();

        // 在声明交换机的时候指定备份交换机
        //Map<String,Object> arguments = new HashMap<String,Object>();
        //arguments.put("alternate-exchange","ALTERNATE_EXCHANGE");
        //channel.exchangeDeclare("TEST_EXCHANGE","topic", false, false, false, arguments);

        // 发送到了默认的交换机上，由于没有任何队列使用这个关键字跟交换机绑定，所以会被退回
        // 第三个参数是设置的mandatory，如果mandatory是false，消息也会被直接丢弃
        //String exchange, String routingKey, BasicProperties props, byte[] body
        channel.basicPublish("","test",properties,"成为更好的你".getBytes());
        TimeUnit.SECONDS.sleep(10);
        channel.close();
        connection.close();
    }
}
