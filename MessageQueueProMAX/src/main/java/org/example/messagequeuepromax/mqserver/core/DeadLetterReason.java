package org.example.messagequeuepromax.mqserver.core;

/**
 * 消息进入死信（或被丢弃）的原因
 * 将来死信队列落地后，该原因会作为元信息附在死信消息上，便于消费死信的一方追溯
 */
public enum DeadLetterReason {
    //消费者显式拒绝且 requeue=false
    REJECTED,
    //重试次数超过队列配置的 x-max-retry 上限
    MAX_RETRY,
    //消费者断开连接，未确认消息被动 requeue（当断连也计入重试并超限时使用）
    CONSUMER_DISCONNECT,
    //消息投递给消费者失败（连接已断等）
    DELIVER_FAILED
}
