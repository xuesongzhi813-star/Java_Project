package org.example.mymessagequeue.common;

import java.io.Serializable;

/**
 * 创建绑定的“特有参数”类
 */
public class BindingDeclareArgument extends BasicArguments implements Serializable {
    private String exchangeName;
    private String queueName;
    private String bindingKey;

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

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }
}
