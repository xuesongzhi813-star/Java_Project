package org.example.mymessagequeue.common;

import java.io.Serializable;

/**
 * 删除交换机API的“特有参数”类
 */
public class ExchangeDeleteArgument extends BasicArguments implements Serializable {
    private String exchangeName;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }
}
