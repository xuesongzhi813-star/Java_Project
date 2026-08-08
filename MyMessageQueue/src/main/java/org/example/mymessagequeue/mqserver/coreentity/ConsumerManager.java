package org.example.mymessagequeue.mqserver.coreentity;

import org.example.mymessagequeue.common.Consumer;
import org.example.mymessagequeue.common.ConsumerEnv;
import org.example.mymessagequeue.common.mqException;
import org.example.mymessagequeue.mqserver.VirtualHost;

import java.util.concurrent.*;

/**
 * 实现消费消息的核心逻辑
 */
public class ConsumerManager {
    //上层“虚拟主机”对象，操作数据
    private VirtualHost virtualHost;
    //线程池，执行回调任务
    private ExecutorService executorService= Executors.newFixedThreadPool(4);
    //存放“令牌”的阻塞队列
    private BlockingQueue<String> tokenQueue=new LinkedBlockingQueue();
    //扫描线程
    private Thread scannerThread=null;

    public ConsumerManager(VirtualHost p){
        this.virtualHost=p;
    }

    //1.通知消费
    public void notifyConsumer(String queueName) throws InterruptedException {
        tokenQueue.put(queueName);
    }

    //2.添加消费者给对应的队列
    public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer) throws mqException {
        //判断，对应的队列是否存在
        MessageQueue queue = virtualHost.getMemoryDataCenter().getQueue(queueName);
        if(queue==null){
            throw new mqException("[ConsumerManager] 对应的队列并不存在:"+queueName);
        }
        //构造消费者对象
        ConsumerEnv consumerEnv=new ConsumerEnv(consumerTag,queueName,autoAck,consumer);
        //添加到队列
        synchronized (queue){
            queue.addConsumerEnv(consumerEnv);
            int n=virtualHost.getMemoryDataCenter().totalMessage(queue);
            //如果队列中有消息，立刻消费
            for (int i = 0; i <n; i++) {
                cosumeMessage(queue);
            }
        }
    }

    private void cosumeMessage(MessageQueue queue) {
    }
}
