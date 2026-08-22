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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        //清空 data 目录：deleteAll 只清数据库表，不清消息文件；
        //残留的 queue_data/queue_stat 会让下个测试 createMessageFile 失败（历史遗留的测试间污染）
        cleanDataDir();
    }

    //删除 data 目录下所有内容（DB+消息文件），下个测试的 init() 会完整重建
    private void cleanDataDir(){
        File dataDir=new File("./data");
        File[] files=dataDir.listFiles();
        if(files==null){
            return;
        }
        for (File file:files){
            deleteRecursively(file);
        }
    }

    private void deleteRecursively(File file){
        if(file.isDirectory()){
            File[] children=file.listFiles();
            if(children!=null){
                for (File child:children){
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
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
//    @Test
//    public void basicPublishTestDIRECT() throws mqException {
//        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes()).isOk();
//        Assertions.assertTrue(ok);
//    }

    //测试消息发送FANOUT
//    @Test
//    public void basicPublishTestFANOUT() throws mqException {
//        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.FANOUT,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.bindingDeclare("testExchange","queueTest","");
//        Assertions.assertTrue(ok);
//        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes()).isOk();
//        Assertions.assertTrue(ok);
//    }

    //测试消息发送TOPIC
//    @Test
//    public void basicPublishTestTOPIC() throws mqException {
//        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.TOPIC,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.bindingDeclare("testExchange","queueTest","aaa.*.ccc");
//        Assertions.assertTrue(ok);
//        ok=virtualHost.basicPublish("testExchange","aaa.cnm.ccc",new BasicProperties(),"hello".getBytes()).isOk();
//        Assertions.assertTrue(ok);
//    }

    //测试订阅消息DIRECT,先发消息，再订阅
//    @Test
//    public void basicSubTestDIRECT1() throws mqException, InterruptedException {
//        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
//        Assertions.assertTrue(ok);
//        MessageQueue queue = virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest");
//        //先发消息，再订阅
//        //发送消息
//        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes()).isOk();
//        Assertions.assertTrue(ok);
//        Thread.sleep(500);
//        ok=virtualHost.basicSubscribe("queueTest", "testConsumerTag", true,new Consumer() {
//            @Override
//            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
//                Assertions.assertEquals("testConsumerTag",consumerTag);
//                Assertions.assertEquals("hello".getBytes(),body);
//            }
//        });
//        Assertions.assertTrue(ok);
//    }
//
//    //测试订阅消息DIRECT，先订阅再发消息
//    @Test
//    public void basicSubTestDIRECT2() throws mqException, InterruptedException {
//        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
//        Assertions.assertTrue(ok);
//        MessageQueue queue = virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest");
//        //先发消息，再订阅
//        ok=virtualHost.basicSubscribe("queueTest", "testConsumerTag", true,new Consumer() {
//            @Override
//            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
//                Assertions.assertEquals("testConsumerTag",consumerTag);
//                Assertions.assertEquals("hello".getBytes(),body);
//            }
//        });
//        Assertions.assertTrue(ok);
//        Thread.sleep(500);
//        //发送消息
//        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes()).isOk();
//        Assertions.assertTrue(ok);
//    }
//
//    //测试订阅消息FANOUT，先发消息，再订阅
//    @Test
//    public void basicSubTestFANOUT1() throws mqException {
//        //FANOUT是绑定的全发，因此多绑定队列
//        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangeType.FANOUT,true,false,null);
//        Assertions.assertTrue(ok);
//
//        boolean queueTest1 = virtualHost.queueDeclare("queueTest1", true, true, false, null);
//        Assertions.assertTrue(queueTest1);
//
//        boolean queueTest2 = virtualHost.queueDeclare("queueTest2", true, true, false, null);
//        Assertions.assertTrue(queueTest2);
//
//        boolean b = virtualHost.bindingDeclare("exchangeTest", "queueTest1", "");
//        Assertions.assertTrue(b);
//        boolean d = virtualHost.bindingDeclare("exchangeTest", "queueTest2", "");
//        Assertions.assertTrue(d);
//
//        //发送消息
//        boolean b1 = virtualHost.basicPublish("exchangeTest", "", new BasicProperties(), "hello".getBytes()).isOk();
//        Assertions.assertTrue(b1);
//
//        //两个订阅
//        MessageQueue queue1=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest1");
//        MessageQueue queue2=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest2");
//        ok=virtualHost.basicSubscribe("queueTest1", "testConsumerTag", true,new Consumer() {
//            @Override
//            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
//                Assertions.assertEquals("testConsumerTag",consumerTag);
//                Assertions.assertEquals("hello".getBytes(),body);
//            }
//        });
//        Assertions.assertTrue(ok);
//        ok=virtualHost.basicSubscribe("queueTest2", "testConsumerTag", true,new Consumer() {
//            @Override
//            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
//                Assertions.assertEquals("testConsumerTag",consumerTag);
//                Assertions.assertEquals("hello".getBytes(),body);
//            }
//        });
//        Assertions.assertTrue(ok);
//    }
//
//    //测试订阅消息FANOUT，先订阅，再发消息
//    @Test
//    public void basicSubTestFANOUT2() throws mqException, InterruptedException {
//        //FANOUT是绑定的全发，因此多绑定队列
//        boolean ok=virtualHost.exchangeDeclare("exchangeTest", exchangeType.FANOUT,true,false,null);
//        Assertions.assertTrue(ok);
//
//        boolean queueTest1 = virtualHost.queueDeclare("queueTest1", true, true, false, null);
//        Assertions.assertTrue(queueTest1);
//
//        boolean queueTest2 = virtualHost.queueDeclare("queueTest2", true, true, false, null);
//        Assertions.assertTrue(queueTest2);
//
//        boolean b = virtualHost.bindingDeclare("exchangeTest", "queueTest1", "");
//        Assertions.assertTrue(b);
//        boolean d = virtualHost.bindingDeclare("exchangeTest", "queueTest2", "");
//        Assertions.assertTrue(d);
//
//        //两个订阅
//        MessageQueue queue1=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest1");
//        MessageQueue queue2=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest2");
//        ok=virtualHost.basicSubscribe("queueTest1", "testConsumerTag", true,new Consumer() {
//            @Override
//            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
//                Assertions.assertEquals("testConsumerTag",consumerTag);
//                Assertions.assertEquals("hello".getBytes(),body);
//            }
//        });
//        Assertions.assertTrue(ok);
//        ok=virtualHost.basicSubscribe("queueTest2", "testConsumerTag", true,new Consumer() {
//            @Override
//            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
//                Assertions.assertEquals("testConsumerTag",consumerTag);
//                Assertions.assertEquals("hello".getBytes(),body);
//            }
//        });
//        Assertions.assertTrue(ok);
//        Thread.sleep(500);
//
//        //发送消息
//        boolean b1 = virtualHost.basicPublish("exchangeTest", "", new BasicProperties(), "hello".getBytes()).isOk();
//        Assertions.assertTrue(b1);
//    }
//
//    //测试订阅消息TOPIC，先发送消息，再订阅
//    @Test
//    public void basicSubTestTOPIC1() throws mqException {
//        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.TOPIC,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.bindingDeclare("testExchange","queueTest","aaa.*.ccc");
//        Assertions.assertTrue(ok);
//        ok=virtualHost.basicPublish("testExchange","aaa.cnm.ccc",new BasicProperties(),"hello".getBytes()).isOk();
//        Assertions.assertTrue(ok);
//        MessageQueue queue=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest");
////订阅
//        boolean basicConsume = virtualHost.basicSubscribe("queueTest","consunmerTagTest", true,new Consumer() {
//            @Override
//            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
//                try {
//                    Assertions.assertEquals("aaa.cnm.ccc", basicProperties.getRoutingKey());
//                    Assertions.assertEquals("hello".getBytes(),bytes);
//                }catch (Error error){
//                    error.printStackTrace();
//                }
//            }
//        });
//        Assertions.assertTrue(basicConsume);
//    }
//
//    //测试订阅消息TOPIC，先订阅，再发消息
//    @Test
//    public void basicSubTestTOPIC2() throws mqException, InterruptedException {
//        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.TOPIC,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.queueDeclare("queueTest",false,true,false,null);
//        Assertions.assertTrue(ok);
//        ok=virtualHost.bindingDeclare("testExchange","queueTest","aaa.*.ccc");
//        Assertions.assertTrue(ok);
//        //订阅
//        MessageQueue queue=virtualHost.getMemoryDataCenter().selectQueue("defaultqueueTest");
//        boolean basicConsume = virtualHost.basicSubscribe("queueTest","consunmerTagTest", true,new Consumer() {
//            @Override
//            public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
//                try {
//                    Assertions.assertEquals("aaa.cnm.ccc", basicProperties.getRoutingKey());
//                    Assertions.assertEquals("hello".getBytes(),bytes);
//                }catch (Error error){
//                    error.printStackTrace();
//                }
//            }
//        });
//        Assertions.assertTrue(basicConsume);
//        Thread.sleep(500);
//
//        ok=virtualHost.basicPublish("testExchange","aaa.cnm.ccc",new BasicProperties(),"hello".getBytes()).isOk();
//        Assertions.assertTrue(ok);
//    }

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
        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes()).isOk();
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        ok=virtualHost.basicSubscribe("queueTest", "testConsumerTag", false,new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
                Assertions.assertEquals("testConsumerTag",consumerTag);
                Assertions.assertEquals("hello".getBytes(),body);
            }
        });
        Assertions.assertTrue(ok);
    }

    //测试拒绝应答（新链路）：requeue 重投 + x-max-retry 超限转死信(未配DLX降级丢弃) + requeue=false 直接丢弃
    @Test
    public void basicRejectTest() throws mqException, InterruptedException {
        //队列参数带 x-max-retry=2：同一消息最多重投 2 次，之后拒绝即转死信
        java.util.Map<String,Object> queueArgs=new java.util.HashMap<>();
        queueArgs.put("x-max-retry",2);
        boolean ok =virtualHost.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);
        Assertions.assertTrue(ok);
        ok=virtualHost.queueDeclare("queueTest",false,true,false,queueArgs);
        Assertions.assertTrue(ok);

        //手动应答模式订阅（拒绝应答只存在于手动应答模式），回调记录收到的 messageId
        List<String> receivedIds= Collections.synchronizedList(new ArrayList<>());
        ok=virtualHost.basicSubscribe("queueTest", "testConsumerTag", false,new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) {
                receivedIds.add(basicProperties.getMessageId());
            }
        });
        Assertions.assertTrue(ok);

        //发送消息，等待投递
        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"hello".getBytes()).isOk();
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        Assertions.assertEquals(1,receivedIds.size());
        String messageId=receivedIds.get(0);

        //第1次拒绝 requeue=true：deliveryCount=0 < 2 -> REQUEUE，消息重新投递
        ok=virtualHost.basicReject("queueTest",messageId,true);
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        Assertions.assertEquals(2,receivedIds.size());

        //第2次拒绝 requeue=true：deliveryCount=1 < 2 -> REQUEUE，再次重投
        ok=virtualHost.basicReject("queueTest",messageId,true);
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        Assertions.assertEquals(3,receivedIds.size());

        //第3次拒绝 requeue=true：deliveryCount=2 >= 2 -> DEAD_LETTER（本期未配DLX，降级丢弃），消息彻底消失不再投递
        ok=virtualHost.basicReject("queueTest",messageId,true);
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        Assertions.assertEquals(3,receivedIds.size());
        //内存中消息已被删干净（selectMessageById 查不到时返回 null）
        Assertions.assertNull(virtualHost.getMemoryDataCenter().selectMessageById(messageId));

        //requeue=false：未配置死信交换机 -> 直接丢弃
        ok=virtualHost.basicPublish("testExchange","defaultqueueTest",new BasicProperties(),"world".getBytes()).isOk();
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        Assertions.assertEquals(4,receivedIds.size());
        String messageId2=receivedIds.get(3);
        ok=virtualHost.basicReject("queueTest",messageId2,false);
        Assertions.assertTrue(ok);
        Thread.sleep(500);
        //消息被丢弃，不再有任何投递
        Assertions.assertEquals(4,receivedIds.size());
        Assertions.assertNull(virtualHost.getMemoryDataCenter().selectMessageById(messageId2));
    }
}
