package org.example.messagequeuepromax.common;

import org.example.messagequeuepromax.mqserver.core.BasicProperties;

import java.io.Serializable;

/**
 * 订阅返回的值
 */
public class SubscribeReturns extends BasicReturns implements Serializable {
    private String consumerTag;
    private BasicProperties basicProperties;
    private byte[] body;
    //消息ID:消费者在回调中通过此字段标识消息,用于显式调用 basicAck/basicReject
    private String messageId;

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
}
