package org.example.messagequeuepromax.mqserver.datacenter;

import org.example.messagequeuepromax.common.ConsumerEnv;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.core.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存数据管理类：
 */
public class MemoryDataCenter {
    //交换机的内存上存储，采取哈希表数据结构
    //key为exchangeName，value为exchange对象
    private ConcurrentHashMap<String, Exchange> exchangeMap=new ConcurrentHashMap<>();

    //队列管理，key为queueName，value为queue对象
    private ConcurrentHashMap<String, MessageQueue> queueMap=new ConcurrentHashMap<>();

    //绑定管理->绑定是交换机<=>队列，则需要有两个数据才能查询到，采取嵌套存储
    //外层哈希：key：exchangeName，value：绑定本交换机的所有队列和绑定
    //内层哈希：key：queueName，value：由exchangeName+queueName唯一确定的绑定
    private ConcurrentHashMap<String,ConcurrentHashMap<String, Binding>> bindingMap=new ConcurrentHashMap<>();

    //用户管理，key：用户名，value：用户对象
    private ConcurrentHashMap<String, UserInfo> userMap=new ConcurrentHashMap<>();

    //消息管理
    //第一个哈希表，管理消息的存储（即只是消息本身同交换机类似），key为messageId，value为message对象
    private ConcurrentHashMap<String, Message> messageMap=new ConcurrentHashMap<>();
    //第二个哈希表，管理消息要发送到的队列关系，key：queueName，value：属于本队列的“消息集合”
    private ConcurrentHashMap<String, LinkedList<Message>> messageBelongMap=new ConcurrentHashMap<>();

    //未确认的消息：key1为queueName，key2为messageId，value为未确认的Message对象
    private ConcurrentHashMap<String,ConcurrentHashMap<String,Message>> unAckMessageMap=new ConcurrentHashMap<>();

    /**
     * 交换机在内存上的增，删，查
     */
    //交换机的增添
    public void insertExchange(Exchange exchange){
        //插入到交换机哈希表中存储即可
        if(exchange==null){
            return;
        }
        exchangeMap.put(exchange.getName(),exchange);
        System.out.println("[MemoryDataCenter] 交换机添加成功:"+exchange.getName());
    }
    //交换机的删除
    public void deleteExchange(String exchangeName){
        exchangeMap.remove(exchangeName);
        System.out.println("[MemoryDataCenter] 交换机删除成功:"+exchangeName);
    }
    //交换机的查询，查询出指定交换机
    public Exchange selectExchange(String exchangeName){
        return exchangeMap.get(exchangeName);
    }

    /**
     * 队列在内存上的增，删，查
     */
    //队列的增添
    public void insertQueue(MessageQueue queue){
        if(queue==null){
            return;
        }
        queueMap.put(queue.getName(),queue);
        System.out.println("[MemoryDataCenter] 队列添加成功:"+queue.getName());
    }
    //队列的删除
    public void deleteQueue(String queueName){
        //删除队列哈希表中本队列
        queueMap.remove(queueName);
        //删除消息-队列哈希表中队列(若能执行到这，则一定已经经过了消息有无的判断)
        messageBelongMap.remove(queueName);
        //删除未确认消息
        unAckMessageMap.remove(queueName);
        System.out.println("[MemoryDataCenter] 队列删除成功:"+queueName);
    }
    //查询指定队列
    public MessageQueue selectQueue(String queueName){
        return queueMap.get(queueName);
    }

    //根据 consumerTag（即channelId）移除消费者订阅：消费者连接断开时清理"死订阅"
    public void removeConsumerByTag(String consumerTag){
        if(consumerTag==null){
            return;
        }
        for(Map.Entry<String,MessageQueue> entry:queueMap.entrySet()){
            MessageQueue queue=entry.getValue();
            queue.deleteConsumerEnvByTag(consumerTag);
        }
    }

    //判断队列中“订阅的消费者集合”是否为空
    public boolean ifEmptyQueue(MessageQueue queue){
        synchronized (queue){
            List<ConsumerEnv> consumerEnvList = queue.getConsumerEnvList();
            if(consumerEnvList==null || consumerEnvList.size()==0){
                return true;
            }
        }
        return false;
    }

    //判断队列中是否还有消息
    public boolean ifEmptyMessageQueue(MessageQueue queue){
        LinkedList<Message> messages = messageBelongMap.get(queue.getName());
        if(messages==null || messages.size()==0){
            return false;
        }
        return true;
    }

    /**
     * 绑定在内存上的增，删，查
     */
    //绑定的插入
    //先用exchangeName查询该交换机绑定是否存在?->有则继续查询这个哈希表中queueName对应绑定是否存在（无则创建一个表）->若都不存在绑定则插入
    public void insertBinding(Binding binding) throws mqException {
        if(binding==null){
            System.out.println("[MemoryDataCenter] 绑定对象为空");
            return;
        }
        //查询这个交换机对应的“第二层绑定表”，存在则得到，不存在则现场创建（因为也要插绑定）
        ConcurrentHashMap<String,Binding> bindingHashMap=bindingMap.computeIfAbsent(binding.getExchangeName()
                ,k->new ConcurrentHashMap<>());
        //查询第二层绑定表中，该绑定对应的queueName是否存在对应数据
        if(bindingHashMap.get(binding.getQueueName())!=null){
            throw new mqException("[MemoryDataCenter] 这个绑定已存在:exchangeName:"+binding.getExchangeName()
            +",queueName:"+binding.getQueueName());
        }
        //加入绑定
        bindingHashMap.put(binding.getQueueName(),binding);
        System.out.println("[MemoryDataCenter] 绑定添加成功:exchangeName:"+binding.getExchangeName() +
                ",queueName:"+binding.getQueueName());
    }
    //绑定的删除
    public void deleteBinding(Binding binding) throws mqException {
        //一层层向内查询删除即可
        ConcurrentHashMap<String, Binding> bingHashMap = bindingMap.get(binding.getExchangeName());
        if (bingHashMap == null) {
            throw new mqException("[MemoryDataCenter] 要删除的绑定不存在:exchangeName:" + binding.getExchangeName());
        }
        if (bingHashMap.get(binding.getQueueName()) == null) {
            throw new mqException("[MemoryDataCenter] 要删除的绑定不存在:queueName:" + binding.getQueueName());
        }
        synchronized (bingHashMap) {
            //查询到了，直接删除绑定即可
            bingHashMap.remove(binding.getQueueName());
            System.out.println("[MemoryDataCenter] 删除绑定成功:exchangeName:" + binding.getExchangeName()
                    + ",queueName:" + binding.getQueueName());
        }
    }

    //获取唯一的绑定
    public Binding getUniqueBinding(String exchangeName,String queueName) throws mqException {
        ConcurrentHashMap<String,Binding> bingHashMap=bindingMap.get(exchangeName);
        if(bingHashMap==null){
            System.out.println("[MemoryDataCenter] 要查询的绑定不存在:exchangeName:"+exchangeName);
            return null;
        }
        Binding binding = bingHashMap.get(queueName);
        return binding;
    }

    //获取到交换机绑定的“队列集合”
    public LinkedList<Binding> getListBinding(String exchangeName) throws mqException {
        LinkedList<Binding> bindingLinkedList = new LinkedList<>();
        ConcurrentHashMap<String, Binding> bindingHashMap = bindingMap.get(exchangeName);
        synchronized (bindingLinkedList) {
            if (bindingHashMap == null) {
                System.out.println("[MemoryDataCenter] 要查询的绑定不存在:exchangeName:" + exchangeName);
                return null;
            }
            //遍历哈希表中值的方法
            for (Map.Entry<String, Binding> entry : bindingHashMap.entrySet()) {
                Binding value = entry.getValue();
                if (value != null) {
                    bindingLinkedList.add(value);
                }
            }
            return bindingLinkedList;
        }
    }

    //获取交换机下的所有“与队列的绑定”的集合
    public ConcurrentHashMap<String,Binding> getExchangeBinding(String exchangeName){
        ConcurrentHashMap<String,Binding> map=bindingMap.get(exchangeName);
        if(map==null){
            System.out.println("[MemoryDataCenter] 要查询的绑定不存在:exchangeName:" + exchangeName);
            return null;
        }
        return map;
    }

    //获取该队列的所有绑定集合
    public List<Binding> getQueueBinding(String queueName){
        List<Binding> bindingList=new ArrayList<>();
        //获取外层键值对的“值”-->获得内层的哈希表
        for(Map.Entry<String,ConcurrentHashMap<String,Binding>> entry:bindingMap.entrySet()){
            ConcurrentHashMap<String, Binding> value = entry.getValue();
            if(value.containsKey(queueName)){
                Binding binding = value.get(queueName);
                //虽然说一个队列和一个交换机只会有一个绑定
                //可能该队列，不止绑定了一个交换机
                bindingList.add(binding);
            }
        }
        return bindingList;
    }
    /**
     * 用户的增，删，查
     */
    //用户的增
    public void insertUser(UserInfo userInfo){
        if(userInfo==null){
            System.out.println("[MemoryDataCenter] 该用户为空");
            return;
        }

        userMap.put(userInfo.getUserName(), userInfo);
        System.out.println("[MemoryDataCenter] 用户增加成功:"+userInfo.getUserName());
    }
    //用户删除
    public void deleteUser(String userName){
        UserInfo userInfo = userMap.get(userName);
        if(userInfo==null){
            System.out.println("[MemoryDataCenter] 该用户为空");
        }
        userMap.remove(userName);
        System.out.println("[MemoryDataCenter] 用户删除成功:"+userName);
    }
    //查询用户
    public UserInfo getUser(String userName){
        return userMap.get(userName);
    }

    /**
     *  实现消息的管理
     */
    //消息的添加
    public void addMessage(Message message) throws mqException {
        if(message==null){
            throw new mqException("[MemoryDataCenter] 该消息对象为空");
        }
        messageMap.put(message.getMessageId(), message);
        System.out.println("[MemoryDataCenter] 消息添加成功messageId:"+message.getMessageId());
    }
    //根据id删除消息
    public void deleteMessageById(String messageId) throws mqException {
        Message message = messageMap.get(messageId);
        if(message==null){
            throw new mqException("[MemoryDataCenter] 该消息不存在");
        }
        messageMap.remove(messageId);
        System.out.println("[MemoryDataCenter] 消息删除成功messageId:"+messageId);
    }
    //根据id查询消息
    public Message selectMessageById(String messageId) throws mqException {
        Message message=messageMap.get(messageId);
        return message;
    }

    /**
     * 关于消息与队列相关的管理实现
     */
    //发送消息到指定队列-->放进每个队列对应的“消息集合”中
    public void sendMessage(MessageQueue queue,Message message) throws mqException {
        synchronized (queue) {
            //查询“消息集合”的存在，若不存在则直接创建一个
            LinkedList<Message> messages = messageBelongMap.computeIfAbsent(queue.getName(), k -> new LinkedList<>());
            //放进消息集合即可
            if (message == null) {
                throw new mqException("[MemoryDataCenter] 要发送的消息对象为空");
            }
            messages.add(message);
            //消息同时登记到 messageMap，否则消费/应答时 deleteMessageById 会因找不到消息而抛"该消息不存在"
            messageMap.put(message.getMessageId(), message);
            System.out.println("[MemoryDataCenter] 发送消息成功:queueName:" + queue.getName()
                    + ",messageId:" + message.getMessageId());
        }
    }

    //消费者从队列中取出消息(取出一个即可)
    public Message pollMessage(MessageQueue queue) throws mqException {
        synchronized (queue) {
            LinkedList<Message> messages = messageBelongMap.get(queue.getName());
            if (messages == null || messages.size() == 0) {
                throw new mqException("[MemoryDataCenter] 该队列中“消息集合”中无消息:queueName:" + queue.getName());
            }
            //取出一个消息，头删，先进先出
            return messages.poll();
        }
    }

    //投递失败后，把消息重新放回队列：pollMessage 只是从队列集合移除，消息仍在 messageMap，无需重复登记
    public void requeueMessage(MessageQueue queue, Message message) throws mqException {
        synchronized (queue) {
            if(queue==null){
                throw new mqException("[MemoryDataCenter] 要重新入队的队列为空");
            }
            if(message==null){
                throw new mqException("[MemoryDataCenter] 要重新入队的消息为空");
            }
            LinkedList<Message> messages = messageBelongMap.computeIfAbsent(queue.getName(), k -> new LinkedList<>());
            //放回队头，尽量保持原有顺序
            messages.addFirst(message);
            System.out.println("[MemoryDataCenter] 消息重新入队成功:queueName:"+queue.getName()+",messageId:"+message.getMessageId());
        }
    }

    //返回指定队列的“消息集合长度”
    public int getMessagesLength(String queueName) throws mqException {
        //获取集合
        LinkedList<Message> messages=messageBelongMap.get(queueName);
        if(messages==null){
            return 0;
        }
        synchronized (messages) {
            return messages.size();
        }
    }

    /**
     * 对“未确认消息”管理实现
     */
    //添加“未确认消息”
    public void addUnAckMessage(String queueName,Message message) throws mqException {
        ConcurrentHashMap<String, Message> concurrentHashMap = unAckMessageMap.computeIfAbsent(queueName,
                k -> new ConcurrentHashMap<>());
        if (message == null) {
            throw new mqException("[MemoryDataCenter] 该消息为空");
        }
        synchronized (concurrentHashMap) {
            concurrentHashMap.put(message.getMessageId(), message);
            System.out.println("[MemoryDataCenter] 未确认消息添加成功:messageId:" + message.getMessageId());
        }
    }
    //删除未确认消息
    public void deleteUnAckMessage(String queueName,String messageId) {
        ConcurrentHashMap<String, Message> stringMessageConcurrentHashMap = unAckMessageMap.get(queueName);
        if (stringMessageConcurrentHashMap == null) {
            return;
        }
        synchronized (stringMessageConcurrentHashMap) {
            Message message = stringMessageConcurrentHashMap.get(messageId);
            if (message == null) {
                return;
            }
            stringMessageConcurrentHashMap.remove(messageId);
            System.out.println("[MemoryDataCenter] 删除未确认数据成功:" + messageId);
        }
    }
    //获取指定“未确认消息”
    public Message getUnAckMessage(String queueName,String messageId){
        ConcurrentHashMap<String, Message> stringMessageConcurrentHashMap = unAckMessageMap.get(queueName);
        if(stringMessageConcurrentHashMap==null){
            return null;
        }
        Message message = stringMessageConcurrentHashMap.get(messageId);
        return message;
    }

    /**
     * 恢复内存数据（重启/服务器问题），从硬盘中恢复数据
     */
    public void recovery(DiskDataCenter diskDataCenter) throws mqException, IOException {
        //先清理内存中残余数据
        exchangeMap.clear();
        queueMap.clear();
        bindingMap.clear();
        messageMap.clear();
        messageBelongMap.clear();
        unAckMessageMap.clear();
        //恢复所有的交换机
        List<Exchange> exchanges = diskDataCenter.selectAllExchange();
        for(Exchange exchange:exchanges){
            //循环遍历所有数据，插入哈希中
            exchangeMap.put(exchange.getName(),exchange);
        }
        //恢复所有的队列
        List<MessageQueue> queues=diskDataCenter.selectAllQueue();
        for (MessageQueue queue:queues){
            queueMap.put(queue.getName(),queue);
        }
        //恢复所有的用户信息
        List<UserInfo> userInfos=diskDataCenter.selectAllUser();
        for (UserInfo userInfo:userInfos){
            userMap.put(userInfo.getUserName(),userInfo);
        }
        //恢复所有绑定
        List<Binding> bindings = diskDataCenter.selectAllBinding();
        for(Binding binding:bindings){
            //先插入嵌套表(因为还未被创建出)，再插入exchange外层表
            ConcurrentHashMap<String, Binding> stringBindingConcurrentHashMap = bindingMap.computeIfAbsent(binding.getExchangeName(), k -> new ConcurrentHashMap<>());
            stringBindingConcurrentHashMap.put(binding.getQueueName(), binding);
            bindingMap.put(binding.getExchangeName(),stringBindingConcurrentHashMap);
        }
        //恢复所有用户（否则服务器重启后 userMap 为空，所有用户都无法登录）
        userMap.clear();
        List<UserInfo> users = diskDataCenter.selectAllUser();
        for (UserInfo user:users){
            userMap.put(user.getUserName(), user);
        }
        //恢复所有消息
        for (MessageQueue queue:queues) {
            //得到每个队列下的所有消息
            List<Message> messages = diskDataCenter.loadMessage(queue.getName());
            for (Message message:messages) {
                //先完成消息信息表恢复
                messageMap.put(message.getMessageId(), message);
            }
            //再完成队列关系“消息集合”表恢复
            messageBelongMap.put(queue.getName(), (LinkedList<Message>) messages);
        }
        //未确认消息丢了也无所谓
        System.out.println("[MemoryDataCenter] 读取硬盘上数据，恢复内存数据成功");
    }
}
