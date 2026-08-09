package org.example.mymessagequeue.common;

import java.io.Serializable;

/**
 * 手动应答API的“特定参数”类
 */
public class BasicAckArgument extends BasicArguments implements Serializable {
    private String queueName;
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
}
