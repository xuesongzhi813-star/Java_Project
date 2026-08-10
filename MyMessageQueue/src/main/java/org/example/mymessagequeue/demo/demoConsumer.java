package org.example.mymessagequeue.demo;

import org.example.mymessagequeue.common.Consumer;
import org.example.mymessagequeue.common.mqException;
import org.example.mymessagequeue.mqclient.Channel;
import org.example.mymessagequeue.mqclient.Connection;
import org.example.mymessagequeue.mqclient.ConnectionFactory;
import org.example.mymessagequeue.mqserver.coreentity.BasicProperties;
import org.example.mymessagequeue.mqserver.coreentity.exchangetype;

import java.io.IOException;

/**
 * 这个类表示一个消费者
 * 通常这个类是一个单独的服务器程序
 */
public class demoConsumer {
    public static void main(String[] args) throws IOException, mqException, InterruptedException {
        System.out.println("启动消费者");

        ConnectionFactory factory=new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(9090);

        Connection connection=factory.newConnection();
        Channel channel=connection.createChannel();

        //创建交换机和队列(存在就不会再创建,都创建一下不影响)
        channel.exchangeDeclare("testExchange", exchangetype.DIRECT,true,false,null);
        channel.queueDeclare("testQueue",true,false,false,null);

        //订阅
        channel.basicConsume("testQueue", true, new Consumer() {
            @Override
            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) throws IOException {
                System.out.println("[消费数据]开始");
                System.out.println("consumerTag:"+conseumerTag);
                System.out.println("basicProperties:"+basicProperties);
                String bodyS=new String(bytes,0,bytes.length);
                System.out.println("body="+bodyS);
                System.out.println("[消费数据]结束");

            }
        });

        //由于不知道“生产者”生产多少，模拟一直“等待消息”，“消费消息”
        while (true){
            Thread.sleep(500);
        }

    }
}
