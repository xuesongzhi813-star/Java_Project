package org.example.mymessagequeue;

import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.example.mymessagequeue.common.mqException;
import org.example.mymessagequeue.mqserver.coreentity.*;
import org.example.mymessagequeue.mqserver.datacenter.DiskDataCenter;
import org.example.mymessagequeue.mqserver.datacenter.MemoryDataCenter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;

@SpringBootTest
public class MemoryDataCenterTest {
    private MemoryDataCenter memoryDataCenter=null;
    /**
     * 单元测试前环境准备：
     * 1.setUp：创建内存存储的数据结构-->依赖于MemoryDataCenter
     * 2.tearDown：对象指空，清空所有内存数据
     */
    @BeforeEach
    public void setUp(){
        memoryDataCenter=new MemoryDataCenter();
    }

    @AfterEach
    public void tearDown(){
        memoryDataCenter=null;
    }

    /**
     * 测试交换机操作：
     * 创建 ->插入->查询+比较引用指向->删除->查是否存在
     */
    private Exchange createExchange(String exchangeName){
        Exchange exchange=new Exchange();
        exchange.setName(exchangeName);
        exchange.setExchageType(exchangetype.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        return exchange;
    }

    @Test
    public void TestExchange(){
        //创建交换机
        Exchange expectedExchange=createExchange("TestExchange");
        //插入
        memoryDataCenter.insertExchange(expectedExchange);
        //查询+比较
        Exchange actuaExchange = memoryDataCenter.getExchange(expectedExchange.getName());
        Assertions.assertEquals(expectedExchange,actuaExchange);
        //删除+查询
        memoryDataCenter.deleteExchange(actuaExchange.getName());
        Exchange again=memoryDataCenter.getExchange(expectedExchange.getName());
        Assertions.assertNull(again);
    }

    /**
     * 测试队列操作：同交换机
     */
    private MessageQueue createQueue(String queueName){
        MessageQueue queue=new MessageQueue();
        queue.setName(queueName);
        queue.setExclusive(false);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        return queue;
    }

    @Test
    public void TestQueue(){
        MessageQueue expectedQueue=createQueue("TestQueue");
        //插入队列
        memoryDataCenter.insertQueue(expectedQueue);
        //查询+比较
        MessageQueue actualQueue = memoryDataCenter.getQueue(expectedQueue.getName());
        Assertions.assertEquals(expectedQueue,actualQueue);
        //删除+查询
        memoryDataCenter.deleteQueue(actualQueue.getName());
        MessageQueue queue = memoryDataCenter.getQueue(expectedQueue.getName());
        Assertions.assertNull(queue);
    }

    /**
     * 测试绑定操作：同上
     */
    private Binding createBinding(){
        Binding expectedBinding=new Binding();
        expectedBinding.setExchangeName("TestExchange");
        expectedBinding.setMessageQueueName("TestQueue");
        expectedBinding.setBindingKey("TestBindingKey");
        return expectedBinding;
    }

    @Test
    public void TestBinding() throws mqException {
        Binding expectedBinding=createBinding();
        //插入+查询+比较
        memoryDataCenter.insertBinding(expectedBinding);
        Binding uniqueBinding = memoryDataCenter.getUniqueBinding(expectedBinding.getExchangeName(), expectedBinding.getMessageQueueName());
        Assertions.assertEquals(expectedBinding.getExchangeName(),uniqueBinding.getExchangeName());
        Assertions.assertEquals(expectedBinding.getMessageQueueName(),uniqueBinding.getMessageQueueName());
        Assertions.assertEquals(expectedBinding.getBindingKey(),uniqueBinding.getBindingKey());
        //删除+查询
        memoryDataCenter.deleteBinding(expectedBinding);
        Binding Binding = memoryDataCenter.getUniqueBinding(expectedBinding.getExchangeName(), expectedBinding.getMessageQueueName());
        Assertions.assertNull(Binding);
    }

    /**
     * 测试消息操作：先测试id-message关联哈希表
     */
    private Message createMessage(String RoutingKey,String body){
        Message message=new Message();
        message.setId();
        message.setRoutingKey(RoutingKey);
        message.setBody(body.getBytes());
        message.setIsValid((byte) 0x1);
        message.setDurable(true);
        return message;
    }

    @Test
    public void testIdAndMessage(){
        Message expectedMessage=createMessage("TestRoutingKey","007");
        //插入
        memoryDataCenter.addMessage(expectedMessage);
        //查询
        Message actualMessage = memoryDataCenter.getById(expectedMessage.getId());
        Assertions.assertEquals(expectedMessage,actualMessage);
        //删除
        memoryDataCenter.deleteById(actualMessage.getId());
        Message byId = memoryDataCenter.getById(expectedMessage.getId());
        Assertions.assertNull(byId);
    }

    /**
     * 测试消息的其他方法
     */
    @Test
    public void TestMessage(){
        //1.发送消息到内存
        MessageQueue queue=createQueue("TestQueue");
        Message expectedMessage=createMessage("TestRoutingKey","007");
        memoryDataCenter.sendMessage(queue,expectedMessage);
        //2.查询内存中消息总数
        int i = memoryDataCenter.totalMessage(queue);
        System.out.println("队列"+queue.getName()+"中消息总数为："+i);
        //3.从内存中取出消息
        Message actualMessage = memoryDataCenter.pollMessage(queue);
        Assertions.assertEquals(expectedMessage,actualMessage);
    }

    /**
     * 测试未确定消息的方法
     */
    @Test
    public void TestUnAckMessage(){
        MessageQueue queue=createQueue("TestQueue");
        Message expectedMessage=createMessage("TestRoutingKey","007");
        //1.添加未确认消息
        memoryDataCenter.addUnAckMessage(queue.getName(),expectedMessage);
        //2.获取指定未确认消息
        Message orderUnAckMessage = memoryDataCenter.getOrderUnAckMessage(queue.getName(), expectedMessage.getId());
        Assertions.assertEquals(expectedMessage,orderUnAckMessage);
        //3.删除未确认消息
        memoryDataCenter.deleteUnAcMessage(queue.getName(),orderUnAckMessage.getId());
        Message UnAckMessage = memoryDataCenter.getOrderUnAckMessage(queue.getName(), expectedMessage.getId());
        Assertions.assertNull(UnAckMessage);
    }

    /**
     * 测试从硬盘中恢复数据：先要能获取硬盘数据（操作数据库）
     */
    @Test
    public void TestRecovery() throws mqException, IOException, ClassNotFoundException {
        //获取spring上下文，从而操作数据库
        MyMessageQueueApplication.context= SpringApplication.run(MyMessageQueueApplication.class);
        DiskDataCenter diskDataCenter=new DiskDataCenter();
        //先在硬盘上构造数据
        diskDataCenter.init();
        Exchange exchange=createExchange("TestExchange");
        MessageQueue queue=createQueue("TestQueue");
        Binding binding=createBinding();
        Message message=createMessage("TestRoutingKey","007");
        diskDataCenter.insertExchange(exchange);
        diskDataCenter.insertQueue(queue);
        diskDataCenter.insertBinding(binding);
        diskDataCenter.sendMessage(queue,message);
        //在内存中恢复数据
        memoryDataCenter.recovery(diskDataCenter);
        //读取比较属性
        Exchange actualExchange = memoryDataCenter.getExchange(exchange.getName());
        MessageQueue actualQueue = memoryDataCenter.getQueue(queue.getName());
        Binding actualBinding = memoryDataCenter.getUniqueBinding(binding.getExchangeName(), binding.getMessageQueueName());
        Message actualMessage = memoryDataCenter.getById(message.getId());
        Assertions.assertEquals(exchange.getName(),actualExchange.getName());
        Assertions.assertEquals(exchange.getExchageType(),actualExchange.getExchageType());
        Assertions.assertEquals(queue.getName(),actualQueue.getName());
        Assertions.assertEquals(queue.isExclusive(),actualQueue.isExclusive());
        Assertions.assertEquals(binding.getExchangeName(),actualBinding.getExchangeName());
        Assertions.assertEquals(binding.getMessageQueueName(),actualBinding.getMessageQueueName());
        Assertions.assertEquals(binding.getBindingKey(),actualBinding.getBindingKey());
        Assertions.assertEquals(message.getId(),actualMessage.getId());
        Assertions.assertEquals(message.getRoutingKey(),actualMessage.getRoutingKey());
        //清空硬盘
        MyMessageQueueApplication.context.close();
        File file=new File("/data");
        FileUtils.deleteDirectory(file);
    }
}
