package org.example.mymessagequeue;

import org.example.mymessagequeue.common.mqException;
import org.example.mymessagequeue.mqserver.coreentity.Message;
import org.example.mymessagequeue.mqserver.coreentity.MessageQueue;
import org.example.mymessagequeue.mqserver.datacenter.MessageFileManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

@SpringBootTest
public class MessageFileManagerTest {
    private MessageFileManager messageFileManager=new MessageFileManager();
    private static final String queueName1="TestQueue1";
    private static final String queueName2="TestQueue2";

    /**
     * 单元测试前环境准备：
     * 1.创建目录和文件（两个队列）
     * 2.删除队列
     */
    @BeforeEach
    public void setUp() throws IOException {
        messageFileManager.createMkdir(queueName1);
        messageFileManager.createMkdir(queueName2);
    }

    @AfterEach
    public void tearDown() throws IOException {
        messageFileManager.deleteMkdirAndFile(queueName1);
        messageFileManager.deleteMkdirAndFile(queueName2);
    }

    /**
     * 测试目录，文件的创建:
     * setUp已完成创建，检查数据文件和统计文件存在即可
     */
    @Test
    public void TestCreateMkdir() throws IOException {
        Assertions.assertEquals(true,messageFileManager.fileExists(queueName1));
        Assertions.assertEquals(true,messageFileManager.fileExists(queueName2));
    }

    /**
     * 测试读写统计文件
     */
    @Test
    public void TestWRStat(){
        //先写入提前配置的统计文件
       MessageFileManager.Stat stat=new MessageFileManager.Stat();
       stat.total=100;
       stat.effect=50;
       //通过封装的反射类调用私有方法
        ReflectionTestUtils.invokeMethod(messageFileManager,"writeStat",queueName1,stat);
        MessageFileManager.Stat readStat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queueName1);
        //比较内容
        Assertions.assertEquals(stat.total,readStat.total);
        Assertions.assertEquals(stat.effect,readStat.effect);
        System.out.println(readStat.total);
        System.out.println(readStat.effect);
    }

    /**
     * 测试发送消息到文件：
     * 参数：queue对象+消息对象
     */
    private MessageQueue createQueue(){
        MessageQueue messageQueue=new MessageQueue();
        messageQueue.setName(queueName1);
        messageQueue.setDurable(true);
        messageQueue.setExclusive(true);
        messageQueue.setAutoDelete(true);
        return messageQueue;
    }

    private Message createMessage(){
        Message message=new Message();
        byte[] bytes={0x01};
        message.setId();
        message.setRoutingKey("TestRoutingKey");
        message.setIsValid((byte) 0x1);
        message.setBody(bytes);
        return message;
    }
    @Test
    public void TestSendMessage() throws mqException, IOException, ClassNotFoundException {
        MessageQueue messageQueue=createQueue();
        Message message=createMessage();
        messageFileManager.sendMessage(messageQueue,message);
        //检查统计文件中参数是否为正确插入后的参数
        MessageFileManager.Stat stat =ReflectionTestUtils.invokeMethod(messageFileManager,"readStat",queueName1);
        Assertions.assertEquals(1,stat.total);
        Assertions.assertEquals(1,stat.effect);
        System.out.println(stat.total);
        System.out.println(stat.effect);
        //检查data文件中元素
        LinkedList<Message> messages=messageFileManager.loadAllMessage(queueName1);
        Assertions.assertEquals(1,messages.size());
        System.out.println(messages.get(0));
    }

    /**
     * 测试加载所有信息：
     */
    @Test
    public void TestLoadAllMessages() throws mqException, IOException, ClassNotFoundException {
        //先创建100个消息，并且存储在本地链表+发送
        LinkedList<Message> messages=new LinkedList<>();
        MessageQueue messageQueue=createQueue();
        for (int i = 0; i <100; i++) {
            Message message=createMessage();
            message.setRoutingKey("TestRoutingKey"+i);
            messageFileManager.sendMessage(messageQueue,message);
            messages.add(message);
        }
        //获取100个消息
        LinkedList<Message> Loadmessages = messageFileManager.loadAllMessage(queueName1);
        //测试是否相等
        Assertions.assertEquals(messages.size(),Loadmessages.size());
        //测试每个数据是否相等
        for (int i = 0; i <100; i++) {
            Assertions.assertEquals(messages.get(i),Loadmessages.get(i));
            System.out.println("插入的数据："+messages.get(i));
            System.out.println("读取到的数据："+Loadmessages.get(i));
        }
    }

    /**
     * 测试删除消息
     */
    @Test
    public void TestDeleteMessage() throws mqException, IOException, ClassNotFoundException {
        //先插入十条消息
        LinkedList<Message> messages=new LinkedList<>();
        MessageQueue messageQueue=createQueue();
        for (int i = 0; i <10; i++) {
            Message message=createMessage();
            message.setRoutingKey("TestRoutingKey"+i);
            messageFileManager.sendMessage(messageQueue,message);
            messages.add(message);
        }
        //删除后三条数据
        messageFileManager.deleteMessage(messageQueue,messages.get(7));
        messageFileManager.deleteMessage(messageQueue,messages.get(8));
        messageFileManager.deleteMessage(messageQueue,messages.get(9));
        //检查前七条数据
        LinkedList<Message> messages1 = messageFileManager.loadAllMessage(queueName1);
        Assertions.assertEquals(7,messages1.size());
        //检查数据内容
        for (int i = 0; i <7; i++) {
            Assertions.assertEquals(messages.get(i),messages1.get(i));
            System.out.println("插入前数据："+messages.get(i));
            System.out.println("读取出数据："+messages1.get(i));
        }
    }

    /**
     * 测试垃圾回收机制：
     */
    @Test
    public void TestGC() throws mqException, IOException, ClassNotFoundException {
        //先插入100条数据
        LinkedList<Message> messages=new LinkedList<>();
        MessageQueue messageQueue=createQueue();
        for (int i = 0; i <100; i++) {
            Message message=createMessage();
            message.setRoutingKey("TestRoutingKey"+i);
            messageFileManager.sendMessage(messageQueue,message);
            messages.add(message);
        }
        //再删除其中的偶数数据
        for (int i = 0; i <100; i+=2) {
            Message message=messages.get(i+1);
            messageFileManager.deleteMessage(messageQueue,message);
        }
        //调用CG复制产生新文件
        messageFileManager.CG(messageQueue);
        //读取新文件消息
        LinkedList<Message> Newmessages=messageFileManager.loadAllMessage(queueName1);
        System.out.println("垃圾回收后文件大小："+Newmessages.size());
        Assertions.assertEquals(messages.size()/2,Newmessages.size());
        MessageFileManager.Stat readStat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queueName1);
        System.out.println("删除后文件消息总量："+readStat.total);
    }
}
