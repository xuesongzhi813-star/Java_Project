package org.example.mymessagequeue;

import org.example.mymessagequeue.mqserver.coreentity.Binding;
import org.example.mymessagequeue.mqserver.coreentity.Exchange;
import org.example.mymessagequeue.mqserver.coreentity.MessageQueue;
import org.example.mymessagequeue.mqserver.coreentity.exchangetype;
import org.example.mymessagequeue.mqserver.datacenter.DataBaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;

@SpringBootTest
public class DataBaseManagerTest {
    private DataBaseManager dataBaseManager=new DataBaseManager();
    /**
     * 准备阶段：
     * 必须要求！！！：单元测试中的用例之间相互独立，互不干扰-->如上一个方法插入一条数据，下一个方法调用同样的数据库就会有影响
     * 解决：1.用例执行前：创建一个新的数据库，准备测试数据
     * 2.用例执行后：删除当前数据库，不给下一个方法测试造成影响
     */
    @BeforeEach
    void setUp(){
        //即初始化，初始化需要MetaMapper参与，因此先注入context
        MyMessageQueueApplication.context = SpringApplication.run(MyMessageQueueApplication.class);
        dataBaseManager.init();
    }
    @AfterEach
    void tearDown(){
        MyMessageQueueApplication.context.close();
        dataBaseManager.deleteAll();
    }

    /**
     * 测试初始化：setUp已经执行，只需要检查是否插入匿名交换机即可，使用断言检查
     */
    @Test
    void initTest(){
        List<Exchange> exchanges = dataBaseManager.selectAllExchange();
        Assertions.assertEquals(exchanges.get(0).getName(),"");
        Assertions.assertEquals(exchanges.get(0).getExchageType(), exchangetype.DIRECT);
        Assertions.assertEquals(exchanges.get(0).isDurable(),true);
        Assertions.assertEquals(exchanges.get(0).isAutoDelete(),true);
    }

    /**
     * 测试插入交换机，并查询出来
     */
    @Test
    void insertExchangeTest(){
        Exchange exchange=createExchange();
        dataBaseManager.insertExchange(exchange);
        //查询，期望结果为2，先前先添加过一个
        List<Exchange> exchanges = dataBaseManager.selectAllExchange();
        Assertions.assertEquals(2,exchanges.size());
        Assertions.assertEquals("TestExchange",exchanges.get(1).getName());
        Assertions.assertEquals(exchangetype.FANOUT,exchanges.get(1).getExchageType());
        Assertions.assertEquals(1,exchanges.get(1).getArguments("aaa"));
        Assertions.assertEquals(2,exchanges.get(1).getArguments("bbb"));
    }

    private Exchange createExchange() {
        Exchange exchange=new Exchange();
        exchange.setName("TestExchange");
        exchange.setExchageType(exchangetype.FANOUT);
        exchange.setDurable(true);
        exchange.setAutoDelete(true);
        exchange.setArguments("aaa",1);
        exchange.setArguments("bbb",2);
        return exchange;
    }

    /**
     * 测试删除交换机：先插入，再删除
     */
    @Test
    void deleteExchangeTest(){
        Exchange exchange=createExchange();
        dataBaseManager.insertExchange(exchange);
        //删除后查询
        dataBaseManager.deleteExchange("TestExchange");
        List<Exchange> exchanges = dataBaseManager.selectAllExchange();
        //期望值为1
        Assertions.assertEquals(1,exchanges.size());
    }

    /**
     * 测试插入队列，并查询出来
     */
    @Test
    void insertQueueTest(){
        MessageQueue queue=createQueue();
        dataBaseManager.insertQueue(queue);
        //查询，期望结果为2，先前先添加过一个
        List<MessageQueue> messageQueues = dataBaseManager.selectAllQueue();
        Assertions.assertEquals(1,messageQueues.size());
        Assertions.assertEquals("TestQueue",messageQueues.get(0).getName());
        Assertions.assertEquals(1,messageQueues.get(0).getArguments("aaa"));
        Assertions.assertEquals(2,messageQueues.get(0).getArguments("bbb"));
    }

    private MessageQueue createQueue() {
        MessageQueue queue=new MessageQueue();
        queue.setName("TestQueue");
        queue.setExclusive(true);
        queue.setDurable(true);
        queue.setAutoDelete(true);
        queue.setArguments("aaa",1);
        queue.setArguments("bbb",2);
        return queue;
    }

    /**
     * 测试删除交换机：先插入，再删除
     */
    @Test
    void deleteQueueTest(){
        MessageQueue queue=createQueue();
        dataBaseManager.insertQueue(queue);
        //删除后查询
        dataBaseManager.deleteQueue("TestQueue");
        List<MessageQueue> messageQueues = dataBaseManager.selectAllQueue();
        //期望值为1
        Assertions.assertEquals(0,messageQueues.size());
    }

    /**
     * 测试插入绑定，并查询出来
     */
    @Test
    void insertBindingTest(){
        Binding binding=createBinding();
        dataBaseManager.insertBinding(binding);
        //查询，期望结果为2，先前先添加过一个
        List<Binding> bindings = dataBaseManager.selectAllBinding();
        Assertions.assertEquals(1,bindings.size());
    }

    private Binding createBinding() {
        Binding binding=new Binding();
        binding.setExchangeName("TestExchange");
        binding.setMessageQueueName("TestQueue");
        binding.setBindingKey("这是测试绑定");
        return binding;
    }

    /**
     * 测试删除交换机：先插入，再删除
     */
    @Test
    void deleteBindingTest(){
        Binding binding=createBinding();
        dataBaseManager.insertBinding(binding);
        //删除后查询
        dataBaseManager.deleteBinding(binding);
        List<Binding> bindings = dataBaseManager.selectAllBinding();
        //期望值为1
        Assertions.assertEquals(0,bindings.size());
    }
}
