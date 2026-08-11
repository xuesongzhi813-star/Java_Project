package org.example.messagequeuepromax.common;

/**
 * 通过枚举类来实现“三种交换机”，用数字来标识交换机
 * 1-DIRECT
 * 2-FANOUT
 * 3-TOPIC
 *
 * 枚举类实现的标准模板：属性+构造方法+基于属性表示的枚举内容
 */
public enum exchangeType {
    DIRECT(1),
    FANOUT(2),
    TOPIC(3);

    private int id;

    exchangeType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}
