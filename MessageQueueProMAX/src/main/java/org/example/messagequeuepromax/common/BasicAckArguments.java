package org.example.messagequeuepromax.common;

import org.example.messagequeuepromax.mqserver.core.Message;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;

import java.io.Serializable;

public class BasicAckArguments extends BasicArguments implements Serializable {
    //旧参数：整个 queue/message 对象（客户端序列化副本，offset 等字段已失效），保留兼容既有调用方/测试
    private MessageQueue queue;
    private Message message;

    //新参数（与 BasicRejectArguments 对齐的轻量化签名）：只传 queueName + messageId，
    //服务端按 messageId 从 unAckMessageMap 反查权威消息对象
    //目标队列名（原始名，不带虚拟主机前缀，服务端自行补前缀）
    private String queueName;
    //要确认的消息ID
    private String messageId;

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

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public MessageQueue getQueue() {
        return queue;
    }

    public void setQueue(MessageQueue queue) {
        this.queue = queue;
    }
}
