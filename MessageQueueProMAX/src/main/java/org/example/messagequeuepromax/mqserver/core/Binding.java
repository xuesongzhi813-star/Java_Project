package org.example.messagequeuepromax.mqserver.core;

import java.io.Serializable;

/**
 * “交换机”和“队列”的绑定关系：
 * 绑定关系作用：交换机收到消息后，发送给哪些队列（有绑定关系的），但具体绑定也要与“交换机类型有关”
 */
public class Binding implements Serializable {
    private String exchangeName;
    private String queueName;
    private String bindingKey;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }
}
