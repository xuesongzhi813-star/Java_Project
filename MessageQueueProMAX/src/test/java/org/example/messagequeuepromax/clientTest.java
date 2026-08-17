package org.example.messagequeuepromax;

import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.example.messagequeuepromax.common.BinaryTool;
import org.example.messagequeuepromax.common.Consumer;
import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqclient.Channel;
import org.example.messagequeuepromax.mqclient.Connection;
import org.example.messagequeuepromax.mqclient.ConnectionFactory;
import org.example.messagequeuepromax.mqserver.BrokerServer;
import org.example.messagequeuepromax.mqserver.core.BasicProperties;
import org.example.messagequeuepromax.mqserver.core.Binding;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;

@SpringBootTest
public class clientTest {
    private BrokerServer brokerServer = null;
    private ConnectionFactory factory = null;
    private Thread t = null;
    private Connection connection = null;

    @BeforeEach
    public void setUp() throws IOException {
        // 1. 先启动服务器
        MessageQueueProMaxApplication.context = SpringApplication.run(MessageQueueProMaxApplication.class);
        brokerServer = new BrokerServer(9090);
        t = new Thread(() -> {
            // 这个 start 方法会进入一个死循环. 使用一个新的线程来运行 start 即可!
            try {
                try {
                    brokerServer.start();
                } catch (mqException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        t.start();

        // 2. 配置 ConnectionFactory
        factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(9090);
    }

    @AfterEach
    public void tearDown() throws IOException {
        // 关闭客户端连接：socket 关闭后，服务端 worker 线程和客户端扫描线程才能从阻塞的 read 上返回退出
        if (connection != null) {
            connection.close();
            connection = null;
        }
        // 停止服务器
        brokerServer.close();
        // t.join();
       MessageQueueProMaxApplication.context.close();

        // 删除必要的文件
        File file = new File("./data");
        FileUtils.deleteDirectory(file);

        factory = null;
    }

    //测试连接
    @Test
    public void testConnection() throws IOException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
    }

    //测试创建channel
    @Test
    public void testCreateChannel() throws IOException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
    }

    //测试交换机相关
    @Test
    public void testExchange() throws IOException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
        boolean ok = channel.exchangeDeclare("testExchange", exchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);
        ok=channel.exchangeDelete("testExchange");
        Assertions.assertTrue(ok);
    }

    //测试队列相关
    @Test
    public void testQueue() throws IOException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
        boolean ok=channel.queueDeclare("testQueue",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=channel.queueDelete("testQueue");
        Assertions.assertTrue(ok);
    }

    //测试绑定相关
    @Test
    public void testBinding() throws IOException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
        boolean ok = channel.exchangeDeclare("testExchange", exchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);
        ok=channel.queueDeclare("testQueue",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=channel.bindingDeclare("testExchange","testQueue","");
        Assertions.assertTrue(ok);
        Binding binding=new Binding();
        binding.setBindingKey("");
        binding.setExchangeName("testExchange");
        binding.setQueueName("testQueue");
        ok=channel.bindingDelete(binding);
        Assertions.assertTrue(ok);
    }

    //测试发送消息
    @Test
    public void sendMessageTest() throws IOException, mqException, InterruptedException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
        boolean ok = channel.exchangeDeclare("testExchange", exchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);
        ok=channel.queueDeclare("testQueue",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=channel.bindingDeclare("testExchange","testQueue","");
        Assertions.assertTrue(ok);
        ok=channel.basicPublish("testExchange","defaulttestQueue",new BasicProperties(),"hello".getBytes());
        Assertions.assertTrue(ok);
        MessageQueue queue=new MessageQueue();
        queue.setName("defaulttestQueue");
        queue.setExclusive(false);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        byte[] bytes="hello".getBytes();
        ok=channel.basicSubscribe(queue, false, new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) throws IOException {
                System.out.println("开始消费消息");
                System.out.println("consumerTag:"+consumerTag);
                Assertions.assertArrayEquals(bytes,body);
                System.out.println("消费消息结束");
            }
        });
        Thread.sleep(500);
        Assertions.assertTrue(ok);
        channel.closeChannel();
        connection.close();
    }
}
