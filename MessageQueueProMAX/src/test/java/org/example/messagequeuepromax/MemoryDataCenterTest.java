package org.example.messagequeuepromax;

import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.core.*;
import org.example.messagequeuepromax.mqserver.datacenter.DiskDataCenter;
import org.example.messagequeuepromax.mqserver.datacenter.MemoryDataCenter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

@SpringBootTest
public class MemoryDataCenterTest {
    MemoryDataCenter memoryDataCenter=null;
    DiskDataCenter diskDataCenter=null;

    @BeforeEach
    public void setUp(){
        //构造测试环境
        //设计到数据库操作，实现mapper
        MessageQueueProMaxApplication.context= SpringApplication.run(MessageQueueProMaxApplication.class);
        memoryDataCenter=new MemoryDataCenter();
        diskDataCenter=new DiskDataCenter();
//        diskDataCenter.init();
    }

    @AfterEach
    public void tearDown(){
        MessageQueueProMaxApplication.context.close();
        memoryDataCenter=null;
        diskDataCenter=null;
    }

    //测试交换机方法
    @Test
    public void testExchange(){
        //先插入一个交换机
        Exchange exchange=createExchange();
        memoryDataCenter.insertExchange(exchange);
        //测试查询交换机 + 比较查询结果
        Exchange exchange1 = memoryDataCenter.selectExchange(exchange.getName());
        Assertions.assertEquals(exchange,exchange1);
        //测试删除交换机
        memoryDataCenter.deleteExchange(exchange1.getName());
        Assertions.assertNull(memoryDataCenter.selectExchange(exchange.getName()));
    }
    public Exchange createExchange(){
        Exchange exchange=new Exchange();
        exchange.setName("testExchange");
        exchange.setExchangeType(exchangeType.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        return exchange;
    }

    //测试队列操作
    @Test
    public void testQueue(){
        //先插入一个队列
        MessageQueue queue=createQueue();
        memoryDataCenter.insertQueue(queue);
        MessageQueue queue1 = memoryDataCenter.selectQueue(queue.getName());
        Assertions.assertEquals(queue,queue1);
        memoryDataCenter.deleteQueue(queue1.getName());
        Assertions.assertNull(memoryDataCenter.selectQueue(queue.getName()));
    }
    public MessageQueue createQueue(){
        MessageQueue messageQueue=new MessageQueue();
        messageQueue.setName("testQueue");
        messageQueue.setDurable(true);
        messageQueue.setExclusive(false);
        messageQueue.setAutoDelete(false);
        return messageQueue;
    }

    //测试绑定操作
    @Test
    public void testBinding() throws mqException {
        Binding binding=createBinding();
        memoryDataCenter.insertBinding(binding);
        //查找唯一队列
        Binding uniqueBinding = memoryDataCenter.getUniqueBinding(binding.getExchangeName(), binding.getQueueName());
        Assertions.assertEquals(binding,uniqueBinding);
        //查找交换机的“绑定集合”
        LinkedList<Binding> listBinding = memoryDataCenter.getListBinding(binding.getExchangeName());
        Assertions.assertEquals(binding,listBinding.get(0));
        memoryDataCenter.deleteBinding(binding);
        Assertions.assertNull(memoryDataCenter.getUniqueBinding(binding.getExchangeName(), binding.getQueueName()));
    }
    public Binding createBinding(){
        Exchange exchange=createExchange();
        MessageQueue queue=createQueue();
        Binding binding=new Binding();
        binding.setExchangeName(exchange.getName());
        binding.setQueueName(queue.getName());
        binding.setBindingKey("");
        return binding;
    }

    //先测试消息存储
    @Test
    public void testMessage() throws mqException {
        Message message= Message.messageFactory("hello".getBytes(),new BasicProperties(),"");
        memoryDataCenter.addMessage(message);
        Message message1 = memoryDataCenter.selectMessageById(message.getMessageId());
        Assertions.assertEquals(message,message1);
        memoryDataCenter.deleteMessageById(message1.getMessageId());
        Assertions.assertNull(memoryDataCenter.selectMessageById(message.getMessageId()));
    }

    //测试队列的“消息集合”
    @Test
    public void testMessageSet() throws mqException {
        Message message=Message.messageFactory("hello".getBytes(),new BasicProperties(),"");
        MessageQueue queue=createQueue();
        //发送消息给指定队列
        memoryDataCenter.sendMessage(queue,message);
        //获取消息集合长度
        int messagesLength = memoryDataCenter.getMessagesLength(queue.getName());
        Assertions.assertEquals(1,messagesLength);
        //获取队列消息
        Message message1 = memoryDataCenter.pollMessage(queue);
        Assertions.assertEquals(message.getMessageId(),message1.getMessageId());
        Assertions.assertEquals(message.getroutingKey(),message1.getroutingKey());
        messagesLength = memoryDataCenter.getMessagesLength(queue.getName());
        Assertions.assertEquals(0,messagesLength);
    }

    //测试“未确认消息”
    @Test
    public void testUnAckMessage() throws mqException {
        Message message= Message.messageFactory("hello".getBytes(),new BasicProperties(),"");
        MessageQueue queue=createQueue();
        memoryDataCenter.addUnAckMessage(queue.getName(),message);
        Message unAckMessage = memoryDataCenter.getUnAckMessage(queue.getName(), message.getMessageId());
        Assertions.assertEquals(message,unAckMessage);
        memoryDataCenter.deleteUnAckMessage(queue.getName(),message.getMessageId());
        unAckMessage=memoryDataCenter.getUnAckMessage(queue.getName(), message.getMessageId());
        Assertions.assertNull(unAckMessage);
    }

    //测试恢复内存数据
    @Test
    public void testRecovery() throws mqException, IOException {
        DiskDataCenter diskDataCenter=new DiskDataCenter();
        //先在硬盘上构造数据
        diskDataCenter.init();
        Exchange exchange=createExchange();
        MessageQueue queue=createQueue();
        Binding binding=createBinding();
        Message message=Message.messageFactory("hello".getBytes(),new BasicProperties(),"");
        diskDataCenter.insertExchange(exchange);
        diskDataCenter.insertQueue(queue);
        diskDataCenter.insertBinding(binding);
        diskDataCenter.writeMessage(queue,message);
        //在内存中恢复数据
        memoryDataCenter.recovery(diskDataCenter);
        //读取比较属性
        Exchange actualExchange = memoryDataCenter.selectExchange(exchange.getName());
        MessageQueue actualQueue = memoryDataCenter.selectQueue(queue.getName());
        Binding actualBinding = memoryDataCenter.getUniqueBinding(binding.getExchangeName(), binding.getQueueName());
        Message actualMessage = memoryDataCenter.selectMessageById(message.getMessageId());
        Assertions.assertEquals(exchange.getName(),actualExchange.getName());
        Assertions.assertEquals(exchange.getExchangeType(),actualExchange.getExchangeType());
        Assertions.assertEquals(queue.getName(),actualQueue.getName());
        Assertions.assertEquals(binding.getExchangeName(),actualBinding.getExchangeName());
        Assertions.assertEquals(binding.getQueueName(),actualBinding.getQueueName());
        Assertions.assertEquals(binding.getBindingKey(),actualBinding.getBindingKey());
        Assertions.assertEquals(message.getMessageId(),actualMessage.getMessageId());
        Assertions.assertEquals(message.getroutingKey(),actualMessage.getroutingKey());
        //清空硬盘
        MessageQueueProMaxApplication.context.close();
        File file=new File("/data");
        FileUtils.deleteDirectory(file);
    }
}
