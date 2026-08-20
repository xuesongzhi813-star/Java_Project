package org.example.messagequeuepromax.common;

import java.io.Serializable;

/**
 * 发送方确认返回类：发送方明确消息已被“持久化”，而非知道发送成功
 */
public class PublishAckReturns extends BasicReturns implements Serializable {
    //消息标识
    private String messageId;
    //目标交换机
    private String exchangeName;
    //routingKey
    private String routingKey;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }
}
