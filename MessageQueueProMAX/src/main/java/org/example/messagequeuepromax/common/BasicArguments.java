package org.example.messagequeuepromax.common;

import java.io.Serializable;

/**
 * 公共参数：基本公共参数+继承实现各个API对应的“独特参数”-->序列化存储在request的payload中
 */
public class BasicArguments implements Serializable {
    private String rid;
    //标识本次“请求”通信是在哪个channel通道进行
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
