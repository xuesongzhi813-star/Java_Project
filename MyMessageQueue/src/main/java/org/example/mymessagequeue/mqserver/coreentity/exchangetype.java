package org.example.mymessagequeue.mqserver.coreentity;

/**
 * 枚举类来标识“交换机类型”
 * 枚举类固定形式：属性+属性对应枚举名+构造方法
 */
public enum exchangetype {
    DIRECT(1),
    FANOUT(2),
    TOPIC(3);

    private Integer type;

    private exchangetype(Integer type){
        this.type=type;
    }
}
