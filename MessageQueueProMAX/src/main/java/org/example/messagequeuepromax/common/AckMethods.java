package org.example.messagequeuepromax.common;

public enum AckMethods {

    ACK(true,"ACK"),
    REQUEUE(true,"REQUEUE"),
    OVER_DEAD_LETTER(true,"OVER_DEAD_LETTER"),//重投次数超额，进入死信队列
    DEAD_LETTER(false,"DEAD_LETTER"),//判定了死信
    DISCARD(false,"DISCARD")//未绑定，直接删除
    ;

    //requeue为分支的“决定值”
    private boolean requeue;
    private String handleMethod;

    //构造方法
     AckMethods(boolean requeue,String handleMethod){
        this.handleMethod=handleMethod;
        this.requeue=requeue;
    }

    public boolean isRequeue() {
        return requeue;
    }

    public String getHandleMethod() {
        return handleMethod;
    }
}
