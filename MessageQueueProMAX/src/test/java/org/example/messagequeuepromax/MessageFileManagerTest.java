package org.example.messagequeuepromax;

import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.core.BasicProperties;
import org.example.messagequeuepromax.mqserver.core.Message;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;
import org.example.messagequeuepromax.mqserver.datacenter.MessageFileManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@SpringBootTest
public class MessageFileManagerTest {
    MessageFileManager messageFileManager=null;
    MessageQueue queue1=new MessageQueue();
    MessageQueue queue2=new MessageQueue();
    @BeforeEach
    public void setUp() throws mqException, IOException {
        //构建测试环境,先创建两个队列对应的文件
        messageFileManager=new MessageFileManager();
        queue1.setName("testQueue1");
        queue2.setName("testQueue2");
        messageFileManager.createMessageFile(queue1.getName());
        messageFileManager.createMessageFile(queue2.getName());
    }

    @AfterEach
    public void tearDown() throws mqException {
        messageFileManager.deleteAllMessage(queue1.getName());
        messageFileManager.deleteAllMessage(queue2.getName());
        messageFileManager=null;
    }

    //测试目录，文件的创建
    @Test
    public void testCreate(){
        //setUp已经创建，检查是否存在即可
        boolean ok = messageFileManager.checkExists(queue1.getName());
        Assertions.assertTrue(ok);
        ok=messageFileManager.checkExists(queue2.getName());
        Assertions.assertTrue(ok);
    }

    //测试统计文件读写
    @Test
    public void testStat(){
        //先读文件
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queue1.getName());
        Assertions.assertEquals(0,stat.total);
        Assertions.assertEquals(0,stat.effect);
        //写文件
        stat.total=10;
        stat.effect=5;
        ReflectionTestUtils.invokeMethod(messageFileManager,"writeStat",queue1.getName(),stat);
        stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queue1.getName());
        Assertions.assertEquals(10,stat.total);
        Assertions.assertEquals(5,stat.effect);
    }

    //测试发送消息到文件
    @Test
    public void testSendMessage() throws mqException, IOException {
        //先读取统计文件内容
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queue1.getName());
        Assertions.assertEquals(0,stat.total);
        Assertions.assertEquals(0,stat.effect);
        //发送一条消息
        Message message=Message.messageFactory("hello".getBytes(),new BasicProperties(),"");
        messageFileManager.writeMessage(queue1,message);
        //再读取统计文件内容
        stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queue1.getName());
        Assertions.assertEquals(1,stat.total);
        Assertions.assertEquals(1,stat.effect);
    }

    //测试读取所有有效消息
    @Test
    public void testLoad() throws mqException, IOException {
        //写入100条消息
        for(int i=0;i<100;i++){
            Message message=Message.messageFactory("hello".getBytes(),new BasicProperties(),"i");
            messageFileManager.writeMessage(queue1,message);
        }
        LinkedList<Message> messages = messageFileManager.loadAllMessage(queue1.getName());
        Assertions.assertEquals(100,messages.size());
        for (int i=0;i<100;i++){
            Assertions.assertEquals("i",messages.get(i).getroutingKey());
        }
    }

    //测试删除指定的消息
    @Test
    public void testDelete() throws mqException, IOException {
        //写入100条消息
        for(int i=0;i<100;i++){
            Message message=Message.messageFactory("hello".getBytes(),new BasicProperties(),"i");
            messageFileManager.writeMessage(queue1,message);
        }
        //先读取统计文件内容
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queue1.getName());
        Assertions.assertEquals(100,stat.total);
        Assertions.assertEquals(100,stat.effect);
        //进行删除操作，删除后三条的消息
        LinkedList<Message> messages = messageFileManager.loadAllMessage(queue1.getName());
        messageFileManager.deleteMessage(queue1,messages.get(97));
        messageFileManager.deleteMessage(queue1,messages.get(98));
        messageFileManager.deleteMessage(queue1,messages.get(99));
        //读取统计文件内容，有效消息会减3
        stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queue1.getName());
        Assertions.assertEquals(100,stat.total);
        Assertions.assertEquals(97,stat.effect);
    }

    //测试垃圾回收机制
    @Test
    public void testGC() throws mqException, IOException {
        //写入100条消息，奇数下标为有效数据，偶数下标为无效数据
        Message message=new Message();
        for(int i=0;i<100;i++){
                message=Message.messageFactory("hello".getBytes(),new BasicProperties(),"i");
                messageFileManager.writeMessage(queue1,message);
                if(i%2==0){
                    messageFileManager.deleteMessage(queue1,message);
                }
        }
        //读取统计文件
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queue1.getName());
        Assertions.assertEquals(100,stat.total);
        Assertions.assertEquals(50,stat.effect);

        //进行GC
        messageFileManager.GC(queue1);
        //读取统计文件
        stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", queue1.getName());
        Assertions.assertEquals(50,stat.total);
        Assertions.assertEquals(50,stat.effect);
    }
}
