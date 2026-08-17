package org.example.messagequeuepromax.demo;

import org.example.messagequeuepromax.common.Consumer;
import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqclient.Channel;
import org.example.messagequeuepromax.mqclient.Connection;
import org.example.messagequeuepromax.mqclient.ConnectionFactory;
import org.example.messagequeuepromax.mqserver.core.BasicProperties;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;

import java.io.IOException;

public class ConsumerDemo {
    public static void main(String[] args) throws IOException, mqException, InterruptedException {
        System.out.println("启动消费者");

        ConnectionFactory factory=new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(9090);

        Connection connection=factory.createConnection();
        Channel channel=connection.createChannel();

        //创建交换机和队列(存在就不会再创建,都创建一下不影响)
        channel.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        channel.queueDeclare("testQueue",true,false,false,null);

        MessageQueue queue=new MessageQueue();
        queue.setName("defaulttestQueue");
        queue.setDurable(true);
        queue.setExclusive(false);
        queue.setAutoDelete(false);

        //订阅
        channel.basicSubscribe(queue, true, new Consumer() {
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
