package org.example.messagequeuepromax;

import org.example.messagequeuepromax.common.Consumer;
import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.VirtualHost;
import org.example.messagequeuepromax.mqserver.core.BasicProperties;
import org.example.messagequeuepromax.mqserver.core.Binding;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class VirtualHostTest {

    VirtualHost virtualHost=null;
    @BeforeEach
    public void setUp(){
        //初始化虚拟机+spring上下文（涉及硬盘操作）
        MessageQueueProMaxApplication.context= SpringApplication.run(MessageQueueProMaxApplication.class);
        virtualHost=new VirtualHost("default");

    }

    @AfterEach
    public void tearDown(){
        //关闭spring上下文+虚拟主机
        MessageQueueProMaxApplication.context.close();
        virtualHost.getDiskDataCenter().getDataBaseManager().deleteAll();
        virtualHost=null;
    }

    //测试创建交换机+删除
    @Test
    public void exchangeTest() throws mqException {
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.exchangeDelete("testExchange");
        Assertions.assertTrue(ok);
    }

    //测试创建队列+删除
    @Test
    public void queueTest() throws mqException {
        boolean ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDelete("queueTest");
        Assertions.assertTrue(ok);
    }

    //测试创建绑定+删除
    @Test
    public void bindingTest() throws mqException {
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.bindingDeclare("testExchange","queueTest","");
        Assertions.assertTrue(ok);
        Binding binding=virtualHost.getMemoryDataCenter().getUniqueBinding("defaulttestExchange","defaultqueueTest");
        ok=virtualHost.bindingDelete(binding);
        Assertions.assertTrue(ok);
    }

    //测试消息发送DIRECT
    @Test
    public void basicPublishTestDIRECT() throws mqException {
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes());
        Assertions.assertTrue(ok);
    }

    //测试消息发送FANOUT
    @Test
    public void basicPublishTestFANOUT() throws mqException {
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.FANOUT,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.bindingDeclare("testExchange","queueTest","");
        Assertions.assertTrue(ok);
        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes());
        Assertions.assertTrue(ok);
    }

    //测试消息发送TOPIC
    @Test
    public void basicPublishTestTOPIC() throws mqException {
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.TOPIC,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.bindingDeclare("testExchange","queueTest","aaa.*.ccc");
        Assertions.assertTrue(ok);
        ok=virtualHost.basicPublish("testExchange","aaa.cnm.ccc",new BasicProperties(),"hello".getBytes());
        Assertions.assertTrue(ok);
    }

    //测试订阅消息DIRECT,先发消息，再订阅
    @Test
    public void basicSubTestDIRECT1() throws mqException, InterruptedException {
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
        Assertions.assertTrue(ok);
        MessageQueue queue = virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest");
        //先发消息，再订阅
        //发送消息
        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes());
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        ok=virtualHost.basicSubscribe(queue, "testConsumerTag", true, new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertEquals("testConsumerTag",consumerTag);
                Assertions.assertEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
    }

    //测试订阅消息DIRECT，先订阅再发消息
    @Test
    public void basicSubTestDIRECT2() throws mqException, InterruptedException {
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
        Assertions.assertTrue(ok);
        MessageQueue queue = virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest");
        //先发消息，再订阅
        ok=virtualHost.basicSubscribe(queue, "testConsumerTag", true, new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertEquals("testConsumerTag",consumerTag);
                Assertions.assertEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        //发送消息
        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes());
        Assertions.assertTrue(ok);
    }

    //测试订阅消息FANOUT，先发消息，再订阅
    @Test
    public void basicSubTestFANOUT1() throws mqException {
        //FANOUT是绑定的全发，因此多绑定队列
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangeType.FANOUT,true,false,null);
        Assertions.assertTrue(ok);

        boolean queueTest1 = virtualHost.queueDeclare("queueTest1", true, true, false, null);
        Assertions.assertTrue(queueTest1);

        boolean queueTest2 = virtualHost.queueDeclare("queueTest2", true, true, false, null);
        Assertions.assertTrue(queueTest2);

        boolean b = virtualHost.bindingDeclare("exchangeTest", "queueTest1", "");
        Assertions.assertTrue(b);
        boolean d = virtualHost.bindingDeclare("exchangeTest", "queueTest2", "");
        Assertions.assertTrue(d);

        //发送消息
        boolean b1 = virtualHost.basicPublish("exchangeTest", "", new BasicProperties(), "hello".getBytes());
        Assertions.assertTrue(b1);

        //两个订阅
        MessageQueue queue1=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest1");
        MessageQueue queue2=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest2");
        ok=virtualHost.basicSubscribe(queue1, "testConsumerTag", true, new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertEquals("testConsumerTag",consumerTag);
                Assertions.assertEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
        ok=virtualHost.basicSubscribe(queue2, "testConsumerTag", true, new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertEquals("testConsumerTag",consumerTag);
                Assertions.assertEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
    }

    //测试订阅消息FANOUT，先订阅，再发消息
    @Test
    public void basicSubTestFANOUT2() throws mqException, InterruptedException {
        //FANOUT是绑定的全发，因此多绑定队列
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangeType.FANOUT,true,false,null);
        Assertions.assertTrue(ok);

        boolean queueTest1 = virtualHost.queueDeclare("queueTest1", true, true, false, null);
        Assertions.assertTrue(queueTest1);

        boolean queueTest2 = virtualHost.queueDeclare("queueTest2", true, true, false, null);
        Assertions.assertTrue(queueTest2);

        boolean b = virtualHost.bindingDeclare("exchangeTest", "queueTest1", "");
        Assertions.assertTrue(b);
        boolean d = virtualHost.bindingDeclare("exchangeTest", "queueTest2", "");
        Assertions.assertTrue(d);

        //两个订阅
        MessageQueue queue1=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest1");
        MessageQueue queue2=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest2");
        ok=virtualHost.basicSubscribe(queue1, "testConsumerTag", true, new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertEquals("testConsumerTag",consumerTag);
                Assertions.assertEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
        ok=virtualHost.basicSubscribe(queue2, "testConsumerTag", true, new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertEquals("testConsumerTag",consumerTag);
                Assertions.assertEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
        Thread.sleep(500);

        //发送消息
        boolean b1 = virtualHost.basicPublish("exchangeTest", "", new BasicProperties(), "hello".getBytes());
        Assertions.assertTrue(b1);
    }

    //测试订阅消息TOPIC，先发送消息，再订阅
    @Test
    public void basicSubTestTOPIC1() throws mqException {
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.TOPIC,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.bindingDeclare("testExchange","queueTest","aaa.*.ccc");
        Assertions.assertTrue(ok);
        ok=virtualHost.basicPublish("testExchange","aaa.cnm.ccc",new BasicProperties(),"hello".getBytes());
        Assertions.assertTrue(ok);
        MessageQueue queue=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest");
//订阅
        boolean basicConsume = virtualHost.basicSubscribe(queue,"consunmerTagTest", true, new Consumer() {
            @Override
            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
                try {
                    Assertions.assertEquals("aaa.cnm.ccc", basicProperties.getRoutingKey());
                    Assertions.assertEquals("hello".getBytes(),bytes);
                }catch (Error error){
                    error.printStackTrace();
                }
            }
        });
        Assertions.assertTrue(basicConsume);
    }

    //测试订阅消息TOPIC，先订阅，再发消息
    @Test
    public void basicSubTestTOPIC2() throws mqException, InterruptedException {
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.TOPIC,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.bindingDeclare("testExchange","queueTest","aaa.*.ccc");
        Assertions.assertTrue(ok);
        //订阅
        MessageQueue queue=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest");
        boolean basicConsume = virtualHost.basicSubscribe(queue,"consunmerTagTest", true, new Consumer() {
            @Override
            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
                try {
                    Assertions.assertEquals("aaa.cnm.ccc", basicProperties.getRoutingKey());
                    Assertions.assertEquals("hello".getBytes(),bytes);
                }catch (Error error){
                    error.printStackTrace();
                }
            }
        });
        Assertions.assertTrue(basicConsume);
        Thread.sleep(500);

        ok=virtualHost.basicPublish("testExchange","aaa.cnm.ccc",new BasicProperties(),"hello".getBytes());
        Assertions.assertTrue(ok);
    }

    //测试手动应答
    @Test
    public void basicAckTest() throws mqException, InterruptedException {
        //随便一个发送消费消息再，设置为false即可
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
        Assertions.assertTrue(ok);
        MessageQueue queue = virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest");
        //先发消息，再订阅
        //发送消息
        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes());
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        ok=virtualHost.basicSubscribe(queue, "testConsumerTag", false, new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertEquals("testConsumerTag",consumerTag);
                Assertions.assertEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
    }
}
