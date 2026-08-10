package org.example.mymessagequeue;

import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.example.mymessagequeue.common.Consumer;
import org.example.mymessagequeue.common.mqException;
import org.example.mymessagequeue.mqclient.Channel;
import org.example.mymessagequeue.mqclient.Connection;
import org.example.mymessagequeue.mqclient.ConnectionFactory;
import org.example.mymessagequeue.mqserver.BrokerServer;
import org.example.mymessagequeue.mqserver.coreentity.BasicProperties;
import org.example.mymessagequeue.mqserver.coreentity.exchangetype;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@SpringBootTest
public class ClientTest {
    private BrokerServer brokerServer = null;
    private ConnectionFactory factory = null;
    private Thread t = null;

    @BeforeEach
    public void setUp() throws IOException {
        // 1. 先启动服务器
        MyMessageQueueApplication.context = SpringApplication.run(MyMessageQueueApplication.class);
        brokerServer = new BrokerServer(9090);
        t = new Thread(() -> {
            // 这个 start 方法会进入一个死循环. 使用一个新的线程来运行 start 即可!
            try {
                brokerServer.start();
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
        // 停止服务器
        brokerServer.close();
        // t.join();
        MyMessageQueueApplication.context.close();

        // 删除必要的文件
        File file = new File("./data");
        FileUtils.deleteDirectory(file);

        factory = null;
    }

    @Test
    public void testConnection() throws IOException {
        Connection connection = factory.newConnection();
        Assertions.assertNotNull(connection);
    }

    //测试创建channel
    @Test
    public void testChannel() throws IOException {
        Connection connection = factory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
    }

    //测试创建和删除交换机
    @Test
    public void exchangeTest() throws IOException {
        Connection connection = factory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean ok = channel.exchangeDeclare("exchangeTest", exchangetype.DIRECT, false, false, null);
        Assertions.assertTrue(ok);

        ok=channel.exchangeDelete("exchangeTest");
        Assertions.assertTrue(ok);

    }

    //测试创建和删除队列
    @Test
    public void queueTest() throws IOException {
        Connection connection = factory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean ok = channel.queueDeclare("queueTest", false, false, false, null);
        Assertions.assertTrue(ok);
        ok=channel.queueDelete("queueTest");
        Assertions.assertTrue(ok);

    }

    //测试绑定的创建和删除
    @Test
    public void bindingTest() throws IOException {
        Connection connection = factory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean ok = channel.exchangeDeclare("exchangeTest", exchangetype.DIRECT, false, false, null);
        Assertions.assertTrue(ok);

        ok = channel.queueDeclare("queueTest", false, false, false, null);
        Assertions.assertTrue(ok);

        ok=channel.BindingDeclare("exchangeTest","queueTest","");
        Assertions.assertTrue(ok);
        ok=channel.BindingDelete("exchangeTest","queueTest") ;
        Assertions.assertTrue(ok);

    }

    //测试发布消息+消费
    @Test
    public void basicTest() throws IOException, mqException, InterruptedException {
        Connection connection = factory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean ok = channel.exchangeDeclare("exchangeTest", exchangetype.DIRECT, false, false, null);
        Assertions.assertTrue(ok);

        ok = channel.queueDeclare("queueTest", false, false, false, null);
        Assertions.assertTrue(ok);

        byte[] bytes="hello".getBytes();
        ok=channel.basicPublish("exchangeTest","queueTest",null,bytes);
        Assertions.assertTrue(ok);
        ok=channel.basicConsume("queueTest", true, new Consumer() {
            @Override
            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] body) throws IOException {
                System.out.println("[消费数据] 开始!");
                System.out.println("consumerTag=" + conseumerTag);
                System.out.println("basicProperties=" + basicProperties);
                Assertions.assertArrayEquals(bytes,body);
                System.out.println("[消费数据] 结束!");
            }
        });
        Thread.sleep(500);
        channel.closeChannel();
        connection.close();
    }

//    private BrokerServer brokerServer=null;
//     private ConnectionFactory connectionFactory=null;
//    Thread t=null;
//
//    @BeforeEach
//    public void setUp() throws IOException {
//        //启动服务器
//        MyMessageQueueApplication.context= SpringApplication.run(MyMessageQueueApplication.class);
//        brokerServer=new BrokerServer(9090);
//        t=new Thread(()->{
//            try {
//                brokerServer.start();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//        t.start();
//
//        //创建连接
//        connectionFactory=new ConnectionFactory();
//        connectionFactory.setHost("127.0.0.1");
//        connectionFactory.setPort(9090);
//    }
//
//    @AfterEach
//    public void tearDown() throws IOException, InterruptedException {
//        //关闭服务器
//        brokerServer.close();
//        MyMessageQueueApplication.context.close();
//        //删除必要文件
//        File file=new File("./data");
//        FileUtils.deleteDirectory(file);
//        connectionFactory=null;
//    }
//
//    //测试连接建立
//    @Test
//    public void ConnectionTest() throws IOException {
//        Connection connection=connectionFactory.newConnection();
//        Assertions.assertNotNull(connection);
//    }
}
