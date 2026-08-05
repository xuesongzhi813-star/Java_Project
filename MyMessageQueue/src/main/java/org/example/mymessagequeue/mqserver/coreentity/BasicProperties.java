package org.example.mymessagequeue.mqserver.coreentity;

import lombok.Data;
import lombok.Getter;

import java.io.Serializable;

/**
 * Message基本属性的“属性类”
 */
@Data
public class BasicProperties implements Serializable {
    //唯一，消息的标识
    private String id;
    //“暗号答案”，归属于消息
    //Topic：相当于“暗号答案”
    //Fanout：无意义
    //Direct：表示“转发的队列名”
    private String routingKey;
    //持久化开关
    @Getter
    private boolean durable=true;
}
