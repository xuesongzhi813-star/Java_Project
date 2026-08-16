package org.example.messagequeuepromax.common;

import java.util.UUID;

/**
 * 消费者对象：消费者具体的实现
 */
public class ConsumerEnv {
    //身份标识（每个消费者的区别标识）
    private String consumerTag;
    //订阅的队列，从这个队列去取消息消费
    private String queueName;
    //应答方式，若为true则“自动应答”（消费完消息，直接自动将消息删除（硬盘+内存+未确定消息集合+消息集合））
    //若为false则“手动应答”需要调用basicAck完成应答
    private boolean autoAck;
    //函数式接口（处理接收到的消息的方法）
    private Consumer consumer;

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }
}
