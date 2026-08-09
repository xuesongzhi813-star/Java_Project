package org.example.mymessagequeue.common;

/**
 * 自定义应用层协议“请求”的格式
 */
public class Request {
    //表示本次请求“调用的API方法哪一个”
    private int type;
    //表示payload的长度
    private int length;
    //表示本次“请求”调用的API的方法“所需的方法参数”（已序列化成二进制数据）
    private byte[] payload;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
