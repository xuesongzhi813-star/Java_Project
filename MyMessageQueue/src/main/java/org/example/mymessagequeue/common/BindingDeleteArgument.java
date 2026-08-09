package org.example.mymessagequeue.common;

import java.io.Serializable;

/**
 * 删除绑定API的“特有参数”类
 */
public class BindingDeleteArgument extends BasicArguments implements Serializable {
    private String exchangeName;
    private String queueName;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
