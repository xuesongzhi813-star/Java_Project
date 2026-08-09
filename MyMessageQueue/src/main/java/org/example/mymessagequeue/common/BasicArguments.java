package org.example.mymessagequeue.common;

import java.io.Serializable;

/**
 * API方法重复出现的参数，整合成“辅助类”：
 * 具体每个API所需的“特有参数”通过继承本类，拓展实现
 */
public class BasicArguments implements Serializable {
    //标识“请求1”和“响应1”对应的标志（相同标志的，则是一对“对应的请求-响应”）
    private String rid;
    //TCP协议通信中的channel通道的标识（哪一个通信）
    private String channelId;

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }
}
