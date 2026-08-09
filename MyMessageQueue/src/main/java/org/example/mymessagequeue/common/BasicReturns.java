package org.example.mymessagequeue.common;

import java.io.Serializable;

/**
 * API方法重复出现的“返回值”，整合成“辅助类”：
 * 具体每个API所需的“特定返回值”通过继承本类，拓展实现
 */
public class BasicReturns implements Serializable {
    private String rid;
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
