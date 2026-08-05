package org.example.mymessagequeue.mqserver.datacenter;

import org.example.mymessagequeue.common.mqException;
import org.example.mymessagequeue.mqserver.coreentity.Binding;
import org.example.mymessagequeue.mqserver.coreentity.Exchange;
import org.example.mymessagequeue.mqserver.coreentity.Message;
import org.example.mymessagequeue.mqserver.coreentity.MessageQueue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 前提：1.数据库管理交换机，队列，绑定。2.本地目录文件管理消息
 * 作为封装类，统一调用接口管理两种存储方式数据的操作
 */
public class DiskDataCenter {
    //创建两个存储管理的实例对象
    private DataBaseManager dataBaseManager=new DataBaseManager();
    private MessageFileManager messageFileManager=new MessageFileManager();

    //初始化
    public void init(){
        dataBaseManager.init();
        messageFileManager.init();
    }

    /**
     * 封装对交换机的增，删，查:
     */
    //插入交换机
    public void insertExchange(Exchange exchange){
        dataBaseManager.insertExchange(exchange);
        System.out.println("交换机插入成功:"+exchange.getName());
    }
    //删除交换机
    public void deleteExchange(String exchangeName){
        dataBaseManager.deleteExchange(exchangeName);
        System.out.println("交换机删除成功:"+exchangeName);
    }
    //查询所有交换机
    public List<Exchange> selectAllExchange(){
       return dataBaseManager.selectAllExchange();
    }

    /**
     * 封装对队列的增，删，查：
     */
    //插入队列
    public void insertQueue(MessageQueue queue){
        dataBaseManager.insertQueue(queue);
        System.out.println("队列插入成功:"+queue.getName());
    }
    //删除队列
    public void deleteQueue(String queueName){
        dataBaseManager.deleteQueue(queueName);
        System.out.println("队列删除成功:"+queueName);
    }
    //查询所有队列
    public List<MessageQueue> selectAllQueue(){
        return dataBaseManager.selectAllQueue();
    }

    /**
     * 封装对绑定的增，删，查：
     */
    //插入绑定
    public void insertBinding(Binding binding){
        dataBaseManager.insertBinding(binding);
        System.out.println("绑定插入成功:");
    }
    //删除绑定
    public void deleteBinding(Binding binding){
        dataBaseManager.deleteBinding(binding);
        System.out.println("绑定删除成功");
    }
    //查询所有绑定
    public List<Binding> selectAllBinding(){
        return dataBaseManager.selectAllBinding();
    }

    /**
     * 封装对消息的操作：发送，删除，读取
     */
    //发送消息
    public void sendMessage(MessageQueue queue, Message message) throws mqException, IOException {
        messageFileManager.sendMessage(queue,message);
    }
    //删除消息
    public void deleteMessage(MessageQueue queue,Message message) throws IOException, ClassNotFoundException, mqException {
        messageFileManager.deleteMessage(queue,message);
        //判断是否触发GC机制（增添了一条无效数据）
        if(messageFileManager.isCG(queue.getName())){
            messageFileManager.CG(queue);
        }
    }
    //读取消息
    public LinkedList<Message> loadAllMessage(String queueName) throws mqException, IOException, ClassNotFoundException {
      return  messageFileManager.loadAllMessage(queueName);
    }
}
