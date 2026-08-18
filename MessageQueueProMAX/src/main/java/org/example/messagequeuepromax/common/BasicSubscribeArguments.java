package org.example.messagequeuepromax.common;

import org.example.messagequeuepromax.mqserver.core.MessageQueue;

import java.io.Serializable;

public class BasicSubscribeArguments extends BasicArguments implements Serializable {
    private String queueName;
    private String consumerTag;
    private boolean autoAck;
    //回调函数无法实现为参数


    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }


    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }
}
