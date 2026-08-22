package org.example.messagequeuepromax.common;

import org.example.messagequeuepromax.mqserver.core.BasicProperties;

import java.io.IOException;

//最终消费者获取消息，就是通过这个函数式接口
@FunctionalInterface
public interface Consumer {
    /**
     * ！！！这个回调函数，只是发送消息给消费者，具体如何消费，要看消费者实例中的逻辑 ,比如本demo中的sout
     * consumerTag：消费者标识，表示“是谁在进行消息的消费”
     * 后两个参数为，消息的参数
     * @param consumerTag
     * @param basicProperties
     * @param body
     */
    public void deliverMessage(String consumerTag, BasicProperties basicProperties,byte[] body) throws IOException, mqException;
}
