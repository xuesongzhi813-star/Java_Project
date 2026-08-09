package org.example.mymessagequeue.common;

import java.io.Serializable;

/**
 * 订阅消息API的“特定参数”类
 */
public class BasicConsumeArgument extends BasicArguments implements Serializable {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;
    //回调函数无法实现为参数

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
}
