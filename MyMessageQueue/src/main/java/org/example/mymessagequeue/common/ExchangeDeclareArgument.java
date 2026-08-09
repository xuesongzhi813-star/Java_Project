package org.example.mymessagequeue.common;

import org.example.mymessagequeue.mqserver.coreentity.exchangetype;

import java.io.Serializable;
import java.util.Map;

/**
 * 创建交换机API所需的“特有参数”：
 */
public class ExchangeDeclareArgument extends BasicArguments implements Serializable {
    private String exchangeName;
    private exchangetype exchangetype;
    private boolean durable;
    private boolean autoDelete;
    private Map<String,Object> arguments;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public exchangetype getExchangetype() {
        return exchangetype;
    }

    public void setExchangetype(exchangetype exchangetype) {
        this.exchangetype = exchangetype;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }
}
