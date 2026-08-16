package org.example.messagequeuepromax.common;

import org.example.messagequeuepromax.mqserver.core.Message;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;

import java.io.Serializable;

public class BasicAckArguments extends BasicArguments implements Serializable {
    private MessageQueue queue;
    private Message message;

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
