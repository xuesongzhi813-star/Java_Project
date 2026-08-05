package org.example.mymessagequeue.mqserver.coreentity;

import lombok.Data;

@Data
public class Binding {
    //交换机标识ID
    private String ExchangeName;
    //队列标识ID
    private String MessageQueueName;
    //当“交换机”类型为“Topic”时有意义，相当于设置“暗号问题”，归属于队列
    private String bindingKey;

    public String getMessageQueueName() {
        return MessageQueueName;
    }

    public void setMessageQueueName(String messageQueueName) {
        MessageQueueName = messageQueueName;
    }

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }

    public String getExchangeName() {
        return ExchangeName;
    }

    public void setExchangeName(String exchangeName) {
        ExchangeName = exchangeName;
    }


}
