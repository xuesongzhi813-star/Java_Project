package org.example.messagequeuepromax.common;

import org.example.messagequeuepromax.mqserver.core.BasicProperties;

//最终消费者消费消息，就是通过这个函数式接口
@FunctionalInterface
public interface Consumer {
    /**
     * consumerTag：消费者标识，表示“是谁在进行消息的消费”
     * 后两个参数为，消息的参数
     * @param consumerTag
     * @param basicProperties
     * @param body
     */
    public void deliverMessage(String consumerTag, BasicProperties basicProperties,byte[] body);
}
