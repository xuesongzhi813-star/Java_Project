package org.example.messagequeuepromax.mqserver.core;

import java.io.Serializable;
import java.util.Map;

public class BasicProperties implements Serializable {
    //消息的id，标识每一条消息
    private String messageId;
    //消息是否可持久化保存
    private boolean durable;
    //消息对应的“应答钥匙”，用以与bindingKey匹配，决定消息发送给哪一个队列
    private String routingKey;
    //消息创建的时间戳
    private long currentTime=System.currentTimeMillis();
    //消息头：承载死信等元信息（key 见 MessageDisposer.X_DEATH 常量）
    //普通消息为 null；死信转投时由服务端写入 DeadLetterInfo，随推送到达客户端
    //新增字段对旧消息文件的反序列化兼容（缺字段取默认值 null）
    private Map<String, Object> headers;

    //消息的死因


    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

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
