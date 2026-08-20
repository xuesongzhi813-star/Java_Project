package org.example.messagequeuepromax.common;

import org.example.messagequeuepromax.mqserver.core.Message;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;

import java.io.Serializable;

/**
 * 拒绝应答参数类
 */
public class BasicRejectArguments extends BasicArguments implements Serializable {
    //目标队列(传queue+message对象模式,供服务端内部调用)
    private MessageQueue queue;
    //拒绝的消息
    private Message message;
    //消息ID(传messageId模式,供客户端回调中调用,免去构造Message/MessageQueue对象的负担)
    private String messageId;
    //是否放回队列重试
    //true消息消费有误造成，需要重试
    //false不想消费，直接丢弃
    private boolean requeue;

    public MessageQueue getQueue() {
        return queue;
    }

    public void setQueue(MessageQueue queue) {
        this.queue = queue;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public boolean isRequeue() {
        return requeue;
    }

    public void setRequeue(boolean requeue) {
        this.requeue = requeue;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
}
