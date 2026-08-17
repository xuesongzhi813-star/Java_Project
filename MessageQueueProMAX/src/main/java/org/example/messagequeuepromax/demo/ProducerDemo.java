package org.example.messagequeuepromax.demo;

import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.mqclient.Channel;
import org.example.messagequeuepromax.mqclient.Connection;
import org.example.messagequeuepromax.mqclient.ConnectionFactory;
import org.example.messagequeuepromax.mqserver.core.BasicProperties;

import java.io.IOException;

public class ProducerDemo {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("启动生产者");
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);

        Connection connection=connectionFactory.createConnection();
        Channel channel=connection.createChannel();

        //创建交换机和队列
        channel.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        channel.queueDeclare("testQueue",true,false,false,null);

        //创建一个消息并发送
        byte[] body="hello".getBytes();
        boolean ok=channel.basicPublish("testExchange","defaulttestQueue",new BasicProperties(),body);
        System.out.println("消息投递完毕:"+ok);

        Thread.sleep(500);
        channel.closeChannel();
        connection.close();
    }
}
