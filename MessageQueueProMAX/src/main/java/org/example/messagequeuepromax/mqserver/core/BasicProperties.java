package org.example.messagequeuepromax.mqserver.core;

import java.io.Serializable;

public class BasicProperties implements Serializable {
    //消息的id，标识每一条消息
    private String messageId;
    //消息是否可持久化保存
    private boolean durable;
    //消息对应的“应答钥匙”，用以与bindingKey匹配，决定消息发送给哪一个队列
    private String routingKey;
    //消息创建的时间戳
    private long currentTime=System.currentTimeMillis();

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public long getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(long currentTime) {
        this.currentTime = currentTime;
    }
}
