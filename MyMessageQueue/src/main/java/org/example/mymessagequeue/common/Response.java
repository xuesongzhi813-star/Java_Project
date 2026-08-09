package org.example.mymessagequeue.common;

public class Response {
    //表示本次“相应”，相应的是“请求调用的相应API方法”是哪一个
    private int type;
    //表示payload的长度
    private int length;
    //表示本次“响应”调用的API方法的“返回结果”（已序列化成二进制数据）
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
