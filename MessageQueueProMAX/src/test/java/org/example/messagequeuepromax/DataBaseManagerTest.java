package org.example.messagequeuepromax;

import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.mqserver.core.Binding;
import org.example.messagequeuepromax.mqserver.core.Exchange;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;
import org.example.messagequeuepromax.mqserver.core.UserInfo;
import org.example.messagequeuepromax.mqserver.datacenter.DataBaseManager;
import org.example.messagequeuepromax.mqserver.mapper.DiskMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
public class DataBaseManagerTest {

    private DataBaseManager dataBaseManager=new DataBaseManager();

    /**
     * 测试环境准备 :初始化+注入mapper
     */
    @BeforeEach
    public void setUp(){
        //注入mapper依赖
        MessageQueueProMaxApplication.context= SpringApplication.run(MessageQueueProMaxApplication.class);
        dataBaseManager.init();
    }

    @AfterEach
    public void tearDown(){
        MessageQueueProMaxApplication.context.close();
        // 清理数据库，下次测试重新开始
        dataBaseManager.deleteAll();
    }

    //测试初始化方法，目标：查询到默认交换机
    @Test
    public void initTest(){
        List<Exchange> exchanges = dataBaseManager.selectAllExchange();
        Exchange exchange = exchanges.get(0);
        Assertions.assertEquals("defaultExchange", exchange.getName());
        Assertions.assertEquals(1, exchange.getExchangeType().getId());
        Assertions.assertEquals(true, exchange.isDurable());
        Assertions.assertEquals(false, exchange.isAutoDelete());
        Assertions.assertEquals(1, exchanges.size());
    }

    //测试插入交换机
    @Test
    public void insertExchangeTest(){
        Exchange exchange=new Exchange();
        exchange.setName("testExchange");
        exchange.setExchangeType(exchangeType.FANOUT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        dataBaseManager.insertExchange(exchange);
        List<Exchange> exchanges = dataBaseManager.selectAllExchange();
        Assertions.assertEquals("testExchange", exchange.getName());
        Assertions.assertEquals(2, exchanges.get(1).getExchangeType().getId());
        Assertions.assertEquals(true, exchanges.get(1).isDurable());
        Assertions.assertEquals(false, exchanges.get(1).isAutoDelete());
        Assertions.assertEquals(2, exchanges.size());
    }

    //测试交换机的删除
    @Test
    public void deleteExchange(){
      //先插入后删除
        Exchange exchange=new Exchange();
        exchange.setName("testExchange");
        exchange.setExchangeType(exchangeType.FANOUT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        dataBaseManager.insertExchange(exchange);
        List<Exchange> exchanges = dataBaseManager.selectAllExchange();
        Assertions.assertEquals("testExchange", exchange.getName());
        Assertions.assertEquals(2, exchanges.get(1).getExchangeType().getId());
        Assertions.assertEquals(true, exchanges.get(1).isDurable());
        Assertions.assertEquals(false, exchanges.get(1).isAutoDelete());
        Assertions.assertEquals(2, exchanges.size());
        dataBaseManager.deleteExchange("testExchange");
        List<Exchange> exchang1 = dataBaseManager.selectAllExchange();
        Assertions.assertEquals(1,exchang1.size());
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

    //测试绑定的插入
    private Binding createBinding(){
        Binding binding=new Binding();
        binding.setExchangeName("exchangeName");
        binding.setQueueName("queueName");
        binding.setBindingKey("");
        return binding;
    }
    @Test
    void insertBindingTest(){
        Binding binding=createBinding();
        dataBaseManager.insertBinding(binding);
        List<Binding> bindings = dataBaseManager.selectAllBinding();
        Assertions.assertEquals("exchangeName",bindings.get(0).getExchangeName());
        Assertions.assertEquals("queueName",bindings.get(0).getQueueName());
        Assertions.assertEquals("",bindings.get(0).getBindingKey());
    }

    //测试删除绑定
    @Test
    void deleteBindingTest(){
        Binding binding=createBinding();
        dataBaseManager.insertBinding(binding);
        List<Binding> bindings = dataBaseManager.selectAllBinding();
        Assertions.assertEquals("exchangeName",bindings.get(0).getExchangeName());
        Assertions.assertEquals("queueName",bindings.get(0).getQueueName());
        Assertions.assertEquals("",bindings.get(0).getBindingKey());
        dataBaseManager.deleteBinding(binding);
        List<Binding> bindings1 = dataBaseManager.selectAllBinding();
        Assertions.assertEquals(0,bindings1.size());
    }

    //测试用户插入
    @Test void insertUserTest(){
        UserInfo userInfo=new UserInfo();
        userInfo.setUserName("admin");
        userInfo.setPassword("admin");
        dataBaseManager.insertUser(userInfo);
        List<UserInfo> userInfos = dataBaseManager.selectAllUser();
        Assertions.assertEquals(1,userInfos.size());
        Assertions.assertEquals("admin",userInfos.get(0).getUserName());
        Assertions.assertEquals("admin",userInfos.get(0).getPassword());
    }

    //测试用户删除
    @Test
    void deleteUserTest(){
        UserInfo userInfo=new UserInfo();
        userInfo.setUserName("admin");
        userInfo.setPassword("admin");
        dataBaseManager.insertUser(userInfo);
        List<UserInfo> userInfos = dataBaseManager.selectAllUser();
        Assertions.assertEquals(1,userInfos.size());
        Assertions.assertEquals("admin",userInfos.get(0).getUserName());
        Assertions.assertEquals("admin",userInfos.get(0).getPassword());
        dataBaseManager.deleteUser("admin");
        List<UserInfo> userInfosList=dataBaseManager.selectAllUser();
        Assertions.assertEquals(0,userInfosList.size());
    }
}
