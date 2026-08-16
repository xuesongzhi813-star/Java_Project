package org.example.messagequeuepromax.common;

import java.io.Serializable;

/**
 * 公共返回值：公共值+继承实现每个API的返回值-->序列化存入response的payload中
 */
public class BasicReturns implements Serializable {
    //标识本次的“请求”<=>“响应”（标识谁与谁一一对应）
    private String rid;
    //标识本次“响应 ”通信是在哪个channel通道进行
    private String channelId;
    private boolean ok;

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

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }
}
