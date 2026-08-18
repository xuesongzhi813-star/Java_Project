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
import java.util.UUID;

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
    public void testConnection() throws IOException, mqException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
    }

    //测试创建channel
    @Test
    public void testCreateChannel() throws IOException, mqException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel("guest","guest");
        Assertions.assertNotNull(channel);
    }

    //测试交换机相关
    @Test
    public void testExchange() throws IOException, mqException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel("guest","guest");
        Assertions.assertNotNull(channel);
        boolean ok = channel.exchangeDeclare("testExchange", exchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);
        ok=channel.exchangeDelete("testExchange");
        Assertions.assertTrue(ok);
    }

    //测试队列相关
    @Test
    public void testQueue() throws IOException, mqException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel("guest","guest");
        Assertions.assertNotNull(channel);
        boolean ok=channel.queueDeclare("testQueue",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=channel.queueDelete("testQueue");
        Assertions.assertTrue(ok);
    }

    //测试绑定相关
    @Test
    public void testBinding() throws IOException, mqException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel("guest","guest");
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
        Channel channel = connection.createChannel("guest","guest");
        Assertions.assertNotNull(channel);
        boolean ok = channel.exchangeDeclare("testExchange", exchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);
        ok=channel.queueDeclare("testQueue",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=channel.bindingDeclare("testExchange","testQueue","");
        Assertions.assertTrue(ok);
        ok=channel.basicPublish("testExchange","defaulttestQueue",new BasicProperties(),"hello".getBytes());
        Assertions.assertTrue(ok);
        byte[] bytes="hello".getBytes();
        ok=channel.basicSubscribe("testQueue", false, new Consumer() {
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

    //测试注册：注册成功后自动用注册时的用户名密码登录，且登录后业务请求可正常执行
    @Test
    public void registerAndLoginTest() throws IOException, mqException, InterruptedException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        //先用默认guest账户登录，创建channel（未认证的channel会被服务器拦截业务请求）
        Channel channel = connection.createChannel("guest","guest");
        Assertions.assertNotNull(channel);
        //注册随机新用户（用UUID保证每次运行用户名不重复，测试可重复执行）
        //register内部：注册成功后直接用注册时的用户名密码登录本channel
        String userName="user-"+ UUID.randomUUID();
        boolean ok=channel.register(userName,"123456");
        Assertions.assertTrue(ok,"注册并自动登录应成功");
        //注册+自动登录后，本channel应能正常执行业务请求（若认证有问题会被拦截返回false）
        ok=channel.exchangeDeclare("testExchange", exchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok,"登录后的channel应能正常执行业务请求");
        channel.closeChannel();
        connection.close();
    }

    //测试登录失败：错误密码创建channel应抛出认证异常，且不影响后续用正确凭证重试
    @Test
    public void loginFailTest() throws IOException, mqException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        //错误密码：createChannel内部登录失败，应抛出mqException（channel已回滚，不会交给调用方）
        Assertions.assertThrows(mqException.class,()->connection.createChannel("guest","wrongpassword"));
        //同一连接用正确凭证重新创建channel，应成功（登录失败只影响该channel，不搞垮连接）
        Channel channel=connection.createChannel("guest","guest");
        Assertions.assertNotNull(channel);
    }

    //测试“先注册后登录”解耦流程：无参createChannel不登录，数据库无该用户也能直接注册
    //（解决“createChannel必须先登录、登录必须先注册”的死循环）
    @Test
    public void registerWithoutExistingAccountTest() throws IOException, mqException, InterruptedException {
        connection=factory.createConnection();
        Assertions.assertNotNull(connection);
        //无参createChannel：只建channel不登录，新用户拿它发注册请求（不依赖guest等任何已有账户）
        Channel channel=connection.createChannel();
        Assertions.assertNotNull(channel);
        String userName="user-"+ UUID.randomUUID();
        boolean ok=channel.register(userName,"123456");
        Assertions.assertTrue(ok,"新用户注册并自动登录应成功");
        //注册+自动登录后，本channel应能正常执行业务请求
        ok=channel.exchangeDeclare("testExchange", exchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok,"注册登录后的channel应能正常执行业务请求");
        channel.closeChannel();
        connection.close();
    }
}
