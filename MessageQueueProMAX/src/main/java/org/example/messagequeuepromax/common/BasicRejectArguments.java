package org.example.messagequeuepromax.common;

import java.io.Serializable;

/**
 * 拒绝应答参数类
 * 轻量化：只传 queueName + messageId + requeue。
 * 之前传整个 queue/message 对象（客户端序列化副本），offset 等字段已失效，且可能与服务器状态不一致；
 * 现在服务端用 messageId 从 unAckMessageMap 反查权威消息对象。
 */
public class BasicRejectArguments extends BasicArguments implements Serializable {
    //目标队列名（原始名，不带虚拟主机前缀，服务端自行补前缀）
    private String queueName;
    //要拒绝的消息ID
    private String messageId;
    //是否放回队列重试
    //true消息消费有误造成，需要重试
    //false不想消费，直接丢弃（队列配置死信交换机时转死信）
    private boolean requeue;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public boolean isRequeue() {
        return requeue;
    }

    public void setRequeue(boolean requeue) {
        this.requeue = requeue;
    }
}
