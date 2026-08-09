package org.example.mymessagequeue.common;

import java.io.Serializable;

/**
 * 删除队列的“特有参数”类
 */
public class QueueDeleteArgument extends BasicArguments implements Serializable {
    private String queueName;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
