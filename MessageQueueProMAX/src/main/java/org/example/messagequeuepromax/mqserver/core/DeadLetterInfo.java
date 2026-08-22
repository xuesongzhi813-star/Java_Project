package org.example.messagequeuepromax.mqserver.core;

import java.io.Serializable;

public class DeadLetterInfo implements Serializable {
   private static final long serialVersionUID=1L;
   //消息死因
    private DeadLetterReason reason;
    //原始队列名
    private String originalQueue;
    //原routingKey
    private String originalRoutingKey;
    //成为死信的时间戳
    private long deadLetterAt;
    //第几次成为死信
    private int count;

    public DeadLetterReason getReason() {
        return reason;
    }

    public void setReason(DeadLetterReason reason) {
        this.reason = reason;
    }

    public String getOriginalQueue() {
        return originalQueue;
    }

    public void setOriginalQueue(String originalQueue) {
        this.originalQueue = originalQueue;
    }

    public String getOriginalRoutingKey() {
        return originalRoutingKey;
    }

    public void setOriginalRoutingKey(String originalRoutingKey) {
        this.originalRoutingKey = originalRoutingKey;
    }

    public long getDeadLetterAt() {
        return deadLetterAt;
    }

    public void setDeadLetterAt(long deadLetterAt) {
        this.deadLetterAt = deadLetterAt;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
