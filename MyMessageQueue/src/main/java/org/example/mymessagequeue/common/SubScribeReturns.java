package org.example.mymessagequeue.common;

import org.example.mymessagequeue.mqserver.coreentity.BasicProperties;

import java.io.Serializable;

/**
 * 订阅返回的“特定返回值”类:
 * 一般是服务器推送回来的消息：consumerTag，basicproperties，body
 */
public class SubScribeReturns extends BasicReturns implements Serializable {
    private String consumerTag;
    private BasicProperties basicProperties;
    private byte[] body;

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
}
