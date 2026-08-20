package org.example.messagequeuepromax.mqserver.core;

import org.example.messagequeuepromax.common.Consumer;
import org.example.messagequeuepromax.common.ConsumerEnv;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.VirtualHost;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 服务器上提供给消费者，操作消费消息的工具类
 */
public class ConsumerManager {
    //本次消费消息是在哪个虚拟主机下进行的（数据的逻辑隔离）
    private VirtualHost virtualHost;
    //线程池，本虚拟主机下可能有很多消费者，通过线程池帮助消费者同时处理调用消费
    private ExecutorService executorService;
    //队列，存放"内部有消息的队列的名字"-->send消息后就会调用notified给这里加入新的"有消息的队列名"
    private BlockingQueue<String> tokens=new LinkedBlockingQueue<>();
    //扫描线程，不断遍历token队列，看哪个队列还有消息，取出来给线程池进行消费消息（执行回调函数）
    private Thread scannerThread=null;

    //构造方法，初始化线程池+扫描线程+规定本次操作在哪个虚拟主机下
    public ConsumerManager(VirtualHost p){
        this.virtualHost=p;
        //初始化线程池
        executorService= Executors.newFixedThreadPool(4);
        //启动扫描线程，不断扫描token队列中哪个队列有消息
        scannerThread=new Thread(()->{
            while (true) {
                //判断当前取到的队列的存在性
                try {
                    String queueName = tokens.take();
                    MessageQueue queue=virtualHost.getMemoryDataCenter().selectQueue(queueName);
                    if(queue==null){
                        throw new mqException("[ConsumerManager] 该队列不存在");
                    }
                    //存在则开始消费消息
                    synchronized (queue){
                        //交给线程池去消费，本线程只负责扫描，找到有消息的队列
                        consumeMessage(queue);
                    }
                } catch (InterruptedException | mqException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        //设置为后台线程，服务器运行结束，本扫描线程也结束
        scannerThread.setDaemon(true);
        scannerThread.start();
    }

    //1.通知消费者消费
    public void notifyConsumer(String queueName) throws InterruptedException {
        //添加给队列，借由"扫描线程"通知消费者调用
        tokens.put(queueName);
    }

    //2.添加消费者给对应的队列
    public void addConsumer(String queueName, String consumerTag, boolean autoAck, Consumer consumer) throws mqException {
        //判断订阅的队列是否存在
        MessageQueue queue = virtualHost.getMemoryDataCenter().selectQueue(queueName);
        if (queue == null) {
            throw new mqException("[ConsumerManager] 订阅的队列并不存在");
        }
        //如果存在，构造消费者对象
        ConsumerEnv consumerEnv = new ConsumerEnv();
        consumerEnv.setConsumerTag(consumerTag);
        consumerEnv.setAutoAck(autoAck);
        //记录订阅的队列名，便于连接断开时清理死订阅
        consumerEnv.setQueueName(queueName);
        consumerEnv.setConsumer(consumer);
        synchronized (queue) {
            //添加到对应队列的消费者集合
            queue.addConsumerEnv(consumerEnv);
            //如果此时队列中有消息，立刻消费
            int n=virtualHost.getMemoryDataCenter().getMessagesLength(queueName);
            for (int i=0;i<n;i++){
                //有多少条消息，全都遍历消费完
                consumeMessage(queue);
            }
        }
    }

    private void consumeMessage(MessageQueue queue) throws mqException {
        //找一个消费者
        ConsumerEnv consumerEnv = queue.selectConsumer();
        if(consumerEnv==null){
            return;
        }
        //取出消息
        Message message = virtualHost.getMemoryDataCenter().pollMessage(queue);
        if(message==null){
            return;
        }
        //通过线程池调用回调函数，发送消息给消费者
        executorService.submit(()->{
            try {
                //先将消息放入未确定消息队列（已经开始消费不知道结果）
                virtualHost.getMemoryDataCenter().addUnAckMessage(queue.getName(),message);
                //调用回调函数消费消息（若目标消费者连接已断开，服务器端 deliverMessage 会抛 IOException）
                //这只是发送给消费者，但是具体消费还未进行
                consumerEnv.getConsumer().deliverMessage(consumerEnv.getConsumerTag(), message.getBasicProperties(), message.getBody());
                //消费完消息响应服务器，看是手动应答，还是自动应答
                if(consumerEnv.isAutoAck()){
                    //消息消费完成，从硬盘+内存+未确定消息集合+消息集合中删除
                    virtualHost.getMemoryDataCenter().deleteUnAckMessage(queue.getName(), message.getMessageId());
                    virtualHost.getMemoryDataCenter().deleteMessageById(message.getMessageId());
                    if(message.getDurable()){
                        virtualHost.getDiskDataCenter().deleteMessage(queue,message);
                    }
                }else {
                    //手动应答:不自动处理,消息留在 unAckMessage 中
                    //登记 consumerTag→messageId 追踪,消费者断开连接时自动 requeue
                    //消费者通过 Channel 显式调用 basicAck(type=0xb) 或 basicReject(type=0xf)
                    virtualHost.addConsumerUnAck(consumerEnv.getConsumerTag(), message.getMessageId());
                }
            } catch (IOException e) {
                //投递失败（消费者连接已断开）：消息不能丢，重新放回队列
                virtualHost.getMemoryDataCenter().deleteUnAckMessage(queue.getName(), message.getMessageId());
                try {
                    virtualHost.getMemoryDataCenter().requeueMessage(queue, message);
                } catch (mqException mqException) {
                    mqException.printStackTrace();
                }
                //该消费者已失联，从订阅集合中移除，避免下次继续选中它
                try {
                    queue.deleteConsumerEnv(consumerEnv);
                } catch (mqException mqException) {
                    mqException.printStackTrace();
                }
                System.out.println("[ConsumerManager] 消息投递失败，已重新入队:queueName:"+queue.getName()+",messageId:"+message.getMessageId());
                //重新通知消费，让消息尽快被活着的消费者消费掉
                try {
                    this.notifyConsumer(queue.getName());
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            } catch (mqException e) {
                e.printStackTrace();
            }
        });
    }
}
