package org.example.messagequeuepromax.mqserver.core;

/**
 * 消息的处置结果：描述一条消息"结束当前生命周期"时应该被如何处理
 * 决策（由调用方根据 requeue开关/队列配置/重试次数等得出）与执行（MessageDisposer）分离，
 * 将来扩展新的处置方式（如死信转投的完整实现）只改执行器，不改各调用点
 */
public enum Disposition {
    //确认消费成功：删除未确认记录+内存消息+硬盘消息
    ACK,
    //重新入队：放回队头，重试计数+1，并立刻通知消费
    REQUEUE,
    //死信：转入死信交换机（未配置死信交换机时降级为 DISCARD）
    DEAD_LETTER,
    //直接丢弃：删除未确认记录+内存消息+硬盘消息
    DISCARD
}
