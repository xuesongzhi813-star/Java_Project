package org.example.messagequeuepromax.mqserver.datacenter;

import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.core.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 统一调度“数据持久化，存储在硬盘上的操作”类:
 * 数据库+消息文件都是在“硬盘上操作”持久化存储，专门定义一个硬盘类，来操作两种数据持久化
 */
public class DiskDataCenter {
    //调度“数据库数据”持久化的对象
    private DataBaseManager dataBaseManager=new DataBaseManager();
    //调度“本地消息文件”持久化的对象
    private MessageFileManager messageFileManager=new MessageFileManager();

    public MessageFileManager getMessageFileManager() {
        return messageFileManager;
    }

    public void setMessageFileManager(MessageFileManager messageFileManager) {
        this.messageFileManager = messageFileManager;
    }

    public DataBaseManager getDataBaseManager() {
        return dataBaseManager;
    }

    public void setDataBaseManager(DataBaseManager dataBaseManager) {
        this.dataBaseManager = dataBaseManager;
    }

    //初始化硬盘，创建好目录和文件
    public void init(){
        dataBaseManager.init();
        messageFileManager.init();
        System.out.println("[DiskDataCenter] 数据库文件及消息文件，初始化完成");
    }

    /**
     * 封装交换机操作：增，删，查
     */
    //插入交换机
    public void insertExchange(Exchange exchange){
        dataBaseManager.insertExchange(exchange);
        System.out.println("[DiskDataCenter] 交换机插入成功:"+exchange.getName());
    }
    //删除交换机
    public void deleteExchange(String exchangeName){
        dataBaseManager.deleteExchange(exchangeName);
        System.out.println("[DiskDataCenter] 交换机删除成功:"+exchangeName);
    }
    //查询交换机
    public List<Exchange> selectAllExchange(){
        return dataBaseManager.selectAllExchange();
    }

    /**
     * 封装队列操作：增，删，查
     */
    //插入队列
    //“消息文件”依附于“队列”存在，因此“插入队列”后，顺便创建“队列文件夹”
    public void insertQueue(MessageQueue queue) throws mqException, IOException {
        dataBaseManager.insertQueue(queue);
        messageFileManager.createMessageFile(queue.getName());
        System.out.println("[DiskDataCenter] 队列插入成功:"+queue.getName());
    }
    //删除队列
    public void deleteQueue(String queueName){
        //先删除数据库中队列
        dataBaseManager.deleteQueue(queueName);
        System.out.println("[DiskDataCenter] DB中队列删除成功:" + queueName);
        //删除队列文件
        try {
            messageFileManager.deleteAllMessage(queueName);
            System.out.println("[DiskDataCenter] 队列文件删除成功:" + queueName);
        } catch (mqException e) {
            System.out.println("[DiskDataCenter] 队列文件删除失败（文件可能不存在）:" + queueName + ", 原因:" + e.getMessage());
        }
    }
    //查询队列
    public List<MessageQueue> selectAllQueue(){
        return dataBaseManager.selectAllQueue();
    }

    /**
     * 封装对绑定的增，删，查：
     */
    //插入绑定
    public void insertBinding(Binding binding){
        dataBaseManager.insertBinding(binding);
        System.out.println("[DiskDataCenter] 绑定插入成功:");
    }
    //删除绑定
    public void deleteBinding(Binding binding){
        dataBaseManager.deleteBinding(binding);
        System.out.println("[DiskDataCenter] 绑定删除成功");
    }
    //查询所有绑定
    public List<Binding> selectAllBinding(){
        return dataBaseManager.selectAllBinding();
    }

    /**
     * 封装对用户的增，删，查
     */
    //插入用户
    public void insertUser(UserInfo userInfo){
        dataBaseManager.insertUser(userInfo);
        System.out.println("[DiskDataCenter] 用户插入成功:"+userInfo.getUserName());
    }
    //删除用户
    public void deleteUser(String userName){
        dataBaseManager.deleteUser(userName);
        System.out.println("[DiskDataCenter] 用户删除成功:"+userName);
    }
    //查询所有用户
    public List<UserInfo> selectAllUser(){
        return dataBaseManager.selectAllUser();
    }

    /**
     * 对消息写入文件，查询有效消息，删除指定消息的封装：
     */
    //消息写入文件
    public void writeMessage(MessageQueue queue, Message message) throws mqException, IOException {
        messageFileManager.writeMessage(queue,message);
    }
    //消息删除
    public void deleteMessage(MessageQueue queue,Message message) throws mqException, IOException {
        messageFileManager.deleteMessage(queue,message);
        //删除了一条消息，有效消息-1，检查是否需要触发“垃圾回收”
        if (messageFileManager.isGC(queue.getName())){
            messageFileManager.GC(queue);
        }
    }
    //查询有效消息
    public List<Message> loadMessage(String queueName) throws mqException, IOException {
        LinkedList<Message> messages = messageFileManager.loadAllMessage(queueName);
        return messages;
    }
}
