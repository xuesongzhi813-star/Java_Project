package org.example.mymessagequeue.demo;

import org.example.mymessagequeue.mqclient.Channel;
import org.example.mymessagequeue.mqclient.Connection;
import org.example.mymessagequeue.mqclient.ConnectionFactory;
import org.example.mymessagequeue.mqserver.coreentity.exchangetype;

import java.io.IOException;

/**
 * 这个类用来表示一个生产者，通常这是一个单独的服务器程序
 */
public class demoProducer {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("启动生产者");
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);

        Connection connection=connectionFactory.newConnection();
        Channel channel=connection.createChannel();

        //创建交换机和队列
        channel.exchangeDeclare("testExchange", exchangetype.DIRECT,true,false,null);
        channel.queueDeclare("testQueue",true,false,false,null);

        //创建一个消息并发送
        byte[] body="hello".getBytes();
        boolean ok=channel.basicPublish("testExchange","testQueue",null,body);
        System.out.println("消息投递完毕:"+ok);

        Thread.sleep(500);
        channel.closeChannel();
        connection.close();
    }
}
