package org.example.mymessagequeue;

import org.example.mymessagequeue.common.Consumer;
import org.example.mymessagequeue.common.mqException;
import org.example.mymessagequeue.mqserver.VirtualHost;
import org.example.mymessagequeue.mqserver.coreentity.BasicProperties;
import org.example.mymessagequeue.mqserver.coreentity.Message;
import org.example.mymessagequeue.mqserver.coreentity.exchangetype;
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
        MyMessageQueueApplication.context= SpringApplication.run(MyMessageQueueApplication.class);
        virtualHost=new VirtualHost("defaultHost");
    }

    @AfterEach
    public void tearDown(){
        MyMessageQueueApplication.context.close();
        virtualHost=null;
    }

    //测试交换机创建
    @Test
    public void exchangeDeclareTest() throws mqException {
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangetype.DIRECT,true,false,null);
        Assertions.assertTrue(ok);
    }

    //测试交换机删除
    @Test
    public void exchangeDeleteTest() throws mqException {
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangetype.DIRECT,true,false,null);
        Assertions.assertTrue(ok);

        boolean the = virtualHost.exchangeDelete("exchangeTest");
        Assertions.assertTrue(the);
    }

    //测试队列创建
    @Test
    public void queueDeclareTest(){
        boolean queueTest = virtualHost.queueDeclare("queueTest", true, true, false, null);
        Assertions.assertTrue(queueTest);
    }

    //测试队列删除
    @Test
    public void queueDeleteTest(){
        boolean queueTest = virtualHost.queueDeclare("queueTest", true, true, false, null);
        Assertions.assertTrue(queueTest);

        boolean ok = virtualHost.queueDelete("queueTest");
        Assertions.assertTrue(ok);
    }

    //测试绑定创建
    @Test
    public void bindingDeclareTest() throws mqException {
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangetype.DIRECT,true,false,null);
        Assertions.assertTrue(ok);

        boolean queueTest = virtualHost.queueDeclare("queueTest", true, true, false, null);
        Assertions.assertTrue(queueTest);

        boolean b = virtualHost.bindingDeclare("exchangeTest", "queueTest", "");
        Assertions.assertTrue(b);

    }

    //测试删除绑定
    @Test
    public void bindingDeleteTest() throws mqException {
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangetype.DIRECT,true,false,null);
        Assertions.assertTrue(ok);

        boolean queueTest = virtualHost.queueDeclare("queueTest", true, true, false, null);
        Assertions.assertTrue(queueTest);

        boolean b = virtualHost.bindingDeclare("exchangeTest", "queueTest", "");
        Assertions.assertTrue(b);

        boolean b1 = virtualHost.bindingDelete("queueTest","exchangeTest");
        Assertions.assertTrue(b1);
    }

    //测试发消息
    @Test
    public void basicPublishTest() throws mqException {
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangetype.DIRECT,true,false,null);
        Assertions.assertTrue(ok);

        boolean queueTest = virtualHost.queueDeclare("queueTest", true, true, false, null);
        Assertions.assertTrue(queueTest);

        Message message=Message.FactoryMessage("hello".getBytes(),new BasicProperties(),"queueTest");
        boolean b = virtualHost.basicPublish("exchangeTest", message.getRoutingKey(), message.getProperties(), message.getBody());
        Assertions.assertTrue(b);
    }

    //测试订阅消息（DIRECT交换机，rk就是队列名）
    @Test
    public void basicSubDIRECT() throws mqException, InterruptedException {
        //先订阅，再发消息
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangetype.DIRECT,true,false,null);
        Assertions.assertTrue(ok);

        boolean queueTest = virtualHost.queueDeclare("queueTest", true, true, false, null);
        Assertions.assertTrue(queueTest);

        boolean b = virtualHost.basicConsume("consumerTagTest", "queueTest", true, new Consumer() {
            @Override
            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
                try {
                    System.out.println("messageId=" + basicProperties.getId());
                    Assertions.assertEquals("queueTest", basicProperties.getRoutingKey());
                    Assertions.assertEquals("body".getBytes(),bytes);
                }catch (Error error){
                    error.printStackTrace();
                }
            }
        });
        Assertions.assertTrue(b);
        Thread.sleep(500);
        //发送消息
        boolean b1 = virtualHost.basicPublish("exchangeTest", "queueTest", null, "body".getBytes());
        Assertions.assertTrue(b1);
    }

    //测试先发消息再订阅
    @Test
    public void basicSendDIRECT() throws mqException, InterruptedException {
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangetype.DIRECT,true,false,null);
        Assertions.assertTrue(ok);

        boolean queueTest = virtualHost.queueDeclare("queueTest", true, true, false, null);
        Assertions.assertTrue(queueTest);

        //发送消息
        boolean b1 = virtualHost.basicPublish("exchangeTest", "queueTest", null, "body".getBytes());
        Assertions.assertTrue(b1);
        Thread.sleep(500);
        //订阅
        boolean b = virtualHost.basicConsume("consumerTagTest", "queueTest", true, new Consumer() {
            @Override
            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
                try {
                    System.out.println("messageId=" + basicProperties.getId());
                    Assertions.assertEquals("queueTest", basicProperties.getRoutingKey());
                    Assertions.assertEquals("body".getBytes(),bytes);
                }catch (Error error){
                    error.printStackTrace();
                }
            }
        });
        Assertions.assertTrue(b);
    }

    //FANOUT测试订阅
    @Test
    public void basicSubFAN() throws mqException {
        //FANOUT是绑定的全发，因此多绑定队列
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangetype.FANOUT,true,false,null);
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
        boolean b1 = virtualHost.basicPublish("exchangeTest", "", null, "body".getBytes());
        Assertions.assertTrue(b1);

        //两个消费者订阅
        boolean basicConsume = virtualHost.basicConsume("consumerTagTest", "queueTest1", true, new Consumer() {
            @Override
            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
                try {
                    System.out.println("messageId=" + basicProperties.getId());
                    Assertions.assertEquals("queueTest1", basicProperties.getRoutingKey());
                    Assertions.assertEquals("body".getBytes(),bytes);
                }catch (Error error){
                    error.printStackTrace();
                }
            }
        });
        Assertions.assertTrue(basicConsume);

        //订阅
        boolean basicConsume1 = virtualHost.basicConsume("consumerTagTest", "queueTest2", true, new Consumer() {
            @Override
            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
                try {
                    System.out.println("messageId=" + basicProperties.getId());
                    Assertions.assertEquals("queueTest2", basicProperties.getRoutingKey());
                    Assertions.assertEquals("body".getBytes(),bytes);
                }catch (Error error){
                    error.printStackTrace();
                }
            }
        });
        Assertions.assertTrue(basicConsume1);

    }

    //测试TOPIC订阅
    @Test
    public void basicSubTOPIC() throws mqException {
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangetype.TOPIC,true,false,null);
        Assertions.assertTrue(ok);

        boolean queueTest = virtualHost.queueDeclare("queueTest", true, true, false, null);
        Assertions.assertTrue(queueTest);

        boolean b = virtualHost.bindingDeclare("exchangeTest", "queueTest", "aaa.*.ccc");
        Assertions.assertTrue(b);
        //发送消息
        boolean b1 = virtualHost.basicPublish("exchangeTest", "aaa.bbb.ccc", null, "body".getBytes());
        Assertions.assertTrue(b1);

        //订阅
        boolean basicConsume = virtualHost.basicConsume("consumerTagTest", "queueTest", true, new Consumer() {
            @Override
            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
                try {
                    System.out.println("messageId=" + basicProperties.getId());
                    Assertions.assertEquals("queueTest", basicProperties.getRoutingKey());
                    Assertions.assertEquals("body".getBytes(),bytes);
                }catch (Error error){
                    error.printStackTrace();
                }
            }
        });
        Assertions.assertTrue(basicConsume);
    }

    //测手动应答
    @Test
    public void basicAckTest() throws mqException {
        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangetype.TOPIC,true,false,null);
        Assertions.assertTrue(ok);

        boolean queueTest = virtualHost.queueDeclare("queueTest", true, true, false, null);
        Assertions.assertTrue(queueTest);

        boolean b = virtualHost.bindingDeclare("exchangeTest", "queueTest", "aaa.*.ccc");
        Assertions.assertTrue(b);
        //发送消息
        boolean b1 = virtualHost.basicPublish("exchangeTest", "aaa.bbb.ccc", null, "body".getBytes());
        Assertions.assertTrue(b1);

        //订阅
        boolean basicConsume = virtualHost.basicConsume("consumerTagTest", "queueTest", false, new Consumer() {
            @Override
            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
                try {
                    System.out.println("messageId=" + basicProperties.getId());
                    Assertions.assertEquals("queueTest", basicProperties.getRoutingKey());
                    Assertions.assertEquals("body".getBytes(),bytes);
                }catch (Error error){
                    error.printStackTrace();
                }
            }
        });
        Assertions.assertTrue(basicConsume);
    }

}
