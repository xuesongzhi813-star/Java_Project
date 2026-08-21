package org.example.messagequeuepromax;

import org.example.messagequeuepromax.common.Consumer;
import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.VirtualHost;
import org.example.messagequeuepromax.mqserver.core.BasicProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;

/**
 * 队列自动删除功能验证：
 * 场景1：最后一个消费者断开 -> autoDelete 队列被删除
 * 场景2：队列有残余消息 -> 不删除
 * 场景3：还有其他消费者 -> 不删除；最后一个断开 -> 删除
 * 注：队列/交换机 durable=false，不产生磁盘文件，避开 data 目录污染问题
 */
public class AutoDeleteTest {

    private VirtualHost virtualHost;

    @BeforeEach
    public void setUp() {
        //DiskDataCenter 依赖 Spring 上下文（DataSource），需要先启动
        MessageQueueProMaxApplication.context = SpringApplication.run(MessageQueueProMaxApplication.class);
        virtualHost = new VirtualHost("default");
    }

    @AfterEach
    public void tearDown() {
        MessageQueueProMaxApplication.context.close();
        virtualHost.getDiskDataCenter().getDataBaseManager().deleteAll();
    }

    private void declareAutoDeleteQueue(String queueName) {
        boolean ok = virtualHost.exchangeDeclare("autoDelExchange", exchangeType.DIRECT, false, false, null);
        Assertions.assertTrue(ok);
        ok = virtualHost.queueDeclare(queueName, false, false, true, null);
        Assertions.assertTrue(ok);
    }

    private void disconnect(String consumerTag) throws InterruptedException {
        //模拟 BrokerServer 断开连接时的两个钩子调用
        virtualHost.getMemoryDataCenter().removeConsumerByTag(consumerTag);
        virtualHost.onConsumerDisconnect(consumerTag);
    }

    //场景1：最后一个消费者断开，无残余消息 -> 队列被自动删除
    @Test
    public void autoDeleteWhenLastConsumerDisconnect() throws InterruptedException {
        declareAutoDeleteQueue("autoDelQ");
        boolean ok = virtualHost.basicSubscribe("autoDelQ", "consumer-1", true,
                (Consumer) (consumerTag, basicProperties, body) -> { });
        Assertions.assertTrue(ok);
        //订阅成功后队列必然存在
        Assertions.assertNotNull(virtualHost.getMemoryDataCenter().selectQueue("defaultautoDelQ"));

        disconnect("consumer-1");

        Assertions.assertNull(virtualHost.getMemoryDataCenter().selectQueue("defaultautoDelQ"),
                "最后一个消费者断开且无残余消息，autoDelete 队列应被自动删除");
    }

    //场景2：队列有残余消息（无人消费）-> 不删除
    @Test
    public void keepQueueWhenMessageRemains() throws InterruptedException, mqException {
        declareAutoDeleteQueue("autoDelQ");
        boolean ok = virtualHost.basicSubscribe("autoDelQ", "consumer-1", true,
                (Consumer) (consumerTag, basicProperties, body) -> { });
        Assertions.assertTrue(ok);
        //消费者先离场（订阅清掉），再让生产者发消息进来 -> 队列中有残余消息且无人消费
        virtualHost.getMemoryDataCenter().removeConsumerByTag("consumer-1");
        ok = virtualHost.basicPublish("autoDelExchange", "defaultautoDelQ", new BasicProperties(), "hello".getBytes()).isOk();
        Assertions.assertTrue(ok);
        Thread.sleep(200);

        disconnect("consumer-1");

        Assertions.assertNotNull(virtualHost.getMemoryDataCenter().selectQueue("defaultautoDelQ"),
                "队列中仍有残余消息，不应被自动删除");
    }

    //场景3：还有别的消费者在 -> 不删；最后一个也断开 -> 删
    @Test
    public void keepQueueWhileOtherConsumerAlive() throws InterruptedException {
        declareAutoDeleteQueue("autoDelQ");
        boolean ok = virtualHost.basicSubscribe("autoDelQ", "consumer-1", true,
                (Consumer) (consumerTag, basicProperties, body) -> { });
        Assertions.assertTrue(ok);
        ok = virtualHost.basicSubscribe("autoDelQ", "consumer-2", true,
                (Consumer) (consumerTag, basicProperties, body) -> { });
        Assertions.assertTrue(ok);

        //消费者1断开：消费者2还在 -> 队列必须保留
        disconnect("consumer-1");
        Assertions.assertNotNull(virtualHost.getMemoryDataCenter().selectQueue("defaultautoDelQ"),
                "仍有消费者2在订阅，队列不应被删除");

        //消费者2也断开：无人无消息 -> 队列删除
        disconnect("consumer-2");
        Assertions.assertNull(virtualHost.getMemoryDataCenter().selectQueue("defaultautoDelQ"),
                "最后一个消费者断开后，队列应被自动删除");
    }
}
