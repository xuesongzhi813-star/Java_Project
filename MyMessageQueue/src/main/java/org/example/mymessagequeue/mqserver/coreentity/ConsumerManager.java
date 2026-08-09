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
        scannerThread=new Thread(()->{
            while (true){
                try {
                    //拿到“令牌”，即队列名
                    String queueName = tokenQueue.take();
                    //查找队列存在
                    MessageQueue queue = virtualHost.getMemoryDataCenter().getQueue(queueName);
                    if(queue==null){
                        throw new mqException("[ConsumerManager] 队列不存在！"+queueName);
                    }
                    //存在则消费消息
                    synchronized (queue) {
                        cosumeMessage(queue);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });
        //设置线程
        scannerThread.setDaemon(true);
        scannerThread.start();
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
        //选定一个消费者
        ConsumerEnv consumerEnv = queue.chooseConsumerEnv();
        //若此时没有消费者，则返回等待消费者
        if(consumerEnv==null){
            return;
        }
        //取出消息，进行消费
        Message message = virtualHost.getMemoryDataCenter().pollMessage(queue);
        if(message==null){
            return;
        }
        //消息提交给线程池执行
        executorService.submit(()->{
            try {
                //消息进入待确认队列
                virtualHost.getMemoryDataCenter().addUnAckMessage(queue.getName(),message);
                //调用“回调函数”执行处理消息的逻辑
                consumerEnv.getConsumer().deliverMessage(consumerEnv.getConsumerTag(), message.getProperties(), message.getBody());
                if(consumerEnv.isAutoAck()){
                    //若是自动应答，则进行删除
                    if(message.getDurable()){
                        //持久化，则在硬盘上删除
                        virtualHost.getDiskDataCenter().deleteMessage(queue,message);
                    }
                    //在未确认集合中删除
                    virtualHost.getMemoryDataCenter().deleteUnAcMessage(queue.getName(), message.getId());
                    //从消息集合中删除
                    virtualHost.getMemoryDataCenter().deleteById(message.getId());
                }
                else {
                    //若是手动应答，调用basicAck
                    virtualHost.basicAck(queue.getName(), message.getId());
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        });
    }
}
