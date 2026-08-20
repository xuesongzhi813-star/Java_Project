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
                //使用本demo专属账户：首次运行自动注册并登录；再次运行(账户已存在)则注册失败，改为直接登录

        //此时，已经注册过，直接登录
//       Channel channel=connection.createChannel("consumerUser","123456");
        Channel channel=connection.createChannel();
        if(!channel.register("consumerUser","123456")){
            channel.login("consumerUser","123456");
        }

        //创建交换机和队列(存在就不会再创建,都创建一下不影响)
        channel.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        channel.queueDeclare("testQueue",true,false,true,null);

        //订阅（直接传“原始”队列名，服务器端会自动补虚拟主机前缀）
        channel.basicSubscribe("testQueue", true, new Consumer() {
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
