package org.example.messagequeuepromax.demo;

import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqclient.Channel;
import org.example.messagequeuepromax.mqclient.Connection;
import org.example.messagequeuepromax.mqclient.ConnectionFactory;
import org.example.messagequeuepromax.mqserver.core.BasicProperties;

import java.io.IOException;

public class Producer2Demo {
    public static void main(String[] args) throws IOException, mqException, InterruptedException {
        System.out.println("启动生产者");
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);

        Connection connection=connectionFactory.createConnection();
                //使用本demo专属账户：首次运行自动注册并登录；再次运行(账户已存在)则注册失败，改为直接登录
        Channel channel=connection.createChannel("producer2User","123456");

        //此时已经注册过，采取直接登录
//        if(!channel.register("producer2User","123456")){
//            channel.login("producer2User","123456");
//        }

        //创建交换机和队列
        channel.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        channel.queueDeclare("testQueue",true,false,true,null);

        //创建一个消息并发送
        byte[] body="hello".getBytes();
        boolean ok=channel.basicPublish("testExchange","defaulttestQueue",new BasicProperties(),body);
        System.out.println("消息投递完毕:"+ok);

        Thread.sleep(500);
        channel.closeChannel();
        connection.close();
    }
}
