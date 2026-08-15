package org.example.messagequeuepromax.mqserver;

import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.core.*;
import org.example.messagequeuepromax.mqserver.datacenter.DiskDataCenter;
import org.example.messagequeuepromax.mqserver.datacenter.MemoryDataCenter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 逻辑隔离不同业务的交换机，队列，绑定，消息数据。
 * 统一API接口给外界调用
 * 注：因为是最外层类，因此对异常不能抛出，必须对异常进行处理
 */
public class VirtualHost {
    private String virtualHostName;
    //调用操作硬盘上数据的对象
    private DiskDataCenter diskDataCenter=new DiskDataCenter();
    //调用操作内存上数据的对象
    private MemoryDataCenter memoryDataCenter=new MemoryDataCenter();
    Router router=new Router();
    //两个锁对象，涉及创建，删除操作，多线程操作可能出现“线程安全”问题
    private Object exchangeLocker=new Object();
    private Object queueLocker=new Object();

    public String getVirtualHostName() {
        return virtualHostName;
    }

    public void setVirtualHostName(String virtualHostName) {
        this.virtualHostName = virtualHostName;
    }

    public DiskDataCenter getDiskDataCenter() {
        return diskDataCenter;
    }

    public void setDiskDataCenter(DiskDataCenter diskDataCenter) {
        this.diskDataCenter = diskDataCenter;
    }

    public MemoryDataCenter getMemoryDataCenter() {
        return memoryDataCenter;
    }

    public void setMemoryDataCenter(MemoryDataCenter memoryDataCenter) {
        this.memoryDataCenter = memoryDataCenter;
    }

    //构造方法，传参虚拟主机名字-->作为区别不同虚拟主机的“唯一标识”
    public VirtualHost(String VirtualHostName) {
        this.virtualHostName=VirtualHostName;

        //虚拟主机被构造后，要初始化内存数据+硬盘数据，便于后续调用操作
        diskDataCenter.init();
        try {
            memoryDataCenter.recovery(diskDataCenter);
        } catch (mqException | IOException e) {
            System.out.println("[VirtualHost] 虚拟主机恢复内存数据失败");
            throw new RuntimeException(e);
        }
    }

    /**
     * 核心操作API
     */
    //1.创建交换机
    public boolean exchangeDeclare(String name, exchangeType exchangeType, boolean durable, boolean autoDelete, Map<String,Object> arguments){
        //每个虚拟主机下，有自己独属的交换机……，通过加上虚拟主机名字来区别
        String exchangeName=virtualHostName+name;
        //查询本交换机是否存在->在内存中查找更快
        try {
            Exchange existExchange=memoryDataCenter.selectExchange(exchangeName);
            if(existExchange!=null){
                System.out.println("[VirtualHost]  交换机已经存在,exchangeName:"+exchangeName);
                return true;
            }
            synchronized (exchangeLocker) {
                //构造交换机对象
                Exchange exchange = new Exchange();
                exchange.setName(exchangeName);
                exchange.setExchangeType(exchangeType);
                exchange.setDurable(durable);
                exchange.setAutoDelete(autoDelete);
                exchange.setArguments(arguments);
                //开始进行创建交换机操作，先进行内存，再进行硬盘
                memoryDataCenter.insertExchange(exchange);
                //判断持久化开关
                if (durable) {
                    diskDataCenter.insertExchange(exchange);
                }
                System.out.println("[VirtualHost] 交换机创建成功，exchangeName:"+exchangeName);
                return true;
        }
        }catch (Exception e){
            System.out.println("[VirtualHost] 交换机创建失败，exchangeName:"+exchangeName);
            return false;
        }
    }

    //2.销毁交换机
    public boolean exchangeDelete(String name) throws mqException {
        //先加上归属的虚拟主机的标识
        String exchangeName=virtualHostName+name;
        //判断要删除的交换机是否存在
        synchronized (exchangeLocker){
            //如果为空
            if(memoryDataCenter.selectExchange(exchangeName)==null){
                throw new mqException("[VirtualHost] 交换机不存在，删除异常，exchangeName:"+exchangeName);
            }
            try {
                //先解除绑定再删除，因为绑定是“交换机”<=>“队列”一方被删除，绑定就不存在
                ConcurrentHashMap<String, Binding> queuesBinding = memoryDataCenter.getExchangeBinding(exchangeName);
                if(queuesBinding!=null && !queuesBinding.isEmpty()){
                    //遍历值，删除
                    for(Map.Entry<String,Binding> entry:queuesBinding.entrySet()){
                        Binding value = entry.getValue();
                        memoryDataCenter.deleteBinding(value);
                        diskDataCenter.deleteBinding(value);
                    }
                }
                //先删除内存
                memoryDataCenter.deleteExchange(exchangeName);
                diskDataCenter.deleteExchange(exchangeName);
                return true;
            }catch (Exception e){
                System.out.println("[VirtualHost] 删除异常，exchangeName:"+exchangeName);
                return false;
            }
        }
    }

    //3.创建队列
    public boolean queueDeclare(String name,boolean exclusive,boolean durable,boolean autoDelete,Map<String,Object> arguments){
        //先加上归属虚拟主机的标识
        String queueName=virtualHostName+name;
        //判断要创建的队列是否存在
        if(memoryDataCenter.getExchangeBinding(queueName)!=null){
            //存在则无需再创建
            System.out.println("[VirtualHost] 队列已经存在，queueName:"+queueName);
            return true;
        }
        synchronized (queueLocker){
            try {
                //不存在进行创建，构造队列
                MessageQueue queue=new MessageQueue();
                queue.setName(queueName);
                queue.setExclusive(exclusive);
                queue.setDurable(durable);
                queue.setAutoDelete(autoDelete);
                queue.setArguments(arguments);
                memoryDataCenter.insertQueue(queue);
                //判断是否持久化
                if(durable){
                    diskDataCenter.insertQueue(queue);
                }
                return true;
            }catch (Exception e){
                System.out.println("[VirtualHost] 队列创建异常，queueName:"+queueName);
                return false;
            }
        }
    }

    //4.销毁队列
    public boolean queueDelete(String name) throws mqException {
        String queueName=virtualHostName+name;
        //先判断删除队列是否存在
        if(memoryDataCenter.selectQueue(queueName)==null){
            throw new mqException("[VirtualHost] 要删除的队列不存在，queueName:"+queueName);
        }
        //进行删除操作
        synchronized (queueLocker){
            try {
                //先进行解绑操作，再进行删除
                List<Binding> queueBinding = memoryDataCenter.getQueueBinding(queueName);
                for (Binding binding:queueBinding){
                    memoryDataCenter.deleteBinding(binding);
                    diskDataCenter.deleteBinding(binding);
                }
                memoryDataCenter.deleteQueue(queueName);
                diskDataCenter.deleteQueue(queueName);
                System.out.println("[VirtualHost] 队列删除成功，queueName:"+queueName);
                return true;
            }catch (Exception e){
                System.out.println("[VirtualHost] 队列删除异常，queueName:"+queueName);
                return false;
            }
        }
    }

    //5.创建绑定
    public boolean bindingDeclare(String exchangeName,String queueName,String bindingKey) throws mqException {
        //对交换机名字/队列名字+归属虚拟主机标识
        String ExchangeName=virtualHostName+exchangeName;
        String QueueName=virtualHostName+queueName;
        //判断绑定是否存在+bindingKey合法
        // (其实不用判断bindingKey合法，因为存在了，肯定接受过后面的bindingKey检测，这里脑抽写了)
        if(memoryDataCenter.getUniqueBinding(ExchangeName,QueueName)!=null){
            if(router.checkBindingKey(memoryDataCenter.getUniqueBinding(ExchangeName,QueueName).getBindingKey())) {
                System.out.println("[VirtualHost] 该绑定已经存在且合法：exchangeName:" + ExchangeName + ",queueName:" + QueueName
                        +",bindingKey:"+bindingKey);
                return true;
            }
            else {
                throw new mqException("[VirtualHost] 该绑定已经存在，但是不合法：exchangeName:" + ExchangeName + ",queueName:" + QueueName
                        +",bindingKey:"+bindingKey);
            }
        }
        //不存在，构造绑定
        Binding binding=new Binding();
        binding.setExchangeName(ExchangeName);
        binding.setQueueName(QueueName);
        if(!router.checkBindingKey(bindingKey)){
            throw new mqException("[VirtualHost] bindKey不合法，创建异常:"+bindingKey);
        }
        binding.setBindingKey(bindingKey);
        //判断交换机+队列是否存在
        if(memoryDataCenter.selectExchange(ExchangeName)==null){
            throw new mqException("[VirtualHost] 该交换机不存在:exchangeName:"+ExchangeName);
        }
        if(memoryDataCenter.selectQueue(QueueName)==null){
            throw new mqException("[VirtualHost] 该队列不存在:queueName:"+QueueName);
        }
        synchronized (exchangeLocker) {
            synchronized (queueLocker) {
                //插入内存+硬盘
                try {
                    memoryDataCenter.insertBinding(binding);
                    diskDataCenter.insertBinding(binding);
                    System.out.println("[VirtualHost] 绑定创建成功:exchangeName:" + ExchangeName + ",queueName:" + QueueName +
                            ",bindingKey:" + bindingKey);
                    return true;
                } catch (Exception e) {
                    System.out.println("[VirtualHost] 绑定创建异常:exchangeName:" + ExchangeName + ",queueName:" + QueueName +
                            ",bindingKey:" + bindingKey);
                    return false;
                }
            }
        }
    }

    //6.销毁绑定
    public boolean bindingDelete(Binding binding) throws mqException {
        //判断绑定是否存在
        Binding orderBinding=memoryDataCenter.getUniqueBinding(binding.getExchangeName(), binding.getQueueName());
        if(orderBinding==null){
            throw new mqException("[VirtualHost] 该绑定不存在:exchangeName:"+binding.getExchangeName()
            +",queueName:"+binding.getQueueName()+",bindingKey:"+binding.getBindingKey());
        }
        //如果交换机/队列先被删除了，会先解绑删除，因此这里不会出现交换机/队列为空删除失败的原因
        //直接进行删除
        synchronized (exchangeLocker){
            synchronized (queueLocker){
                try{
                    memoryDataCenter.deleteBinding(binding);
                    diskDataCenter.deleteBinding(binding);
                    System.out.println("[VirtualHost] 绑定删除成功:exchangeName:"+binding.getExchangeName()
                            +",queueName:"+binding.getQueueName()+",bindingKey:"+binding.getBindingKey());
                    return true;
                }catch (Exception e){
                    System.out.println("[VirtualHost] 该绑定删除失败:exchangeName:"+binding.getExchangeName()
                            +",queueName:"+binding.getQueueName()+",bindingKey:"+binding.getBindingKey());;
                    return false;
                }
            }
        }
    }

    //7.发送消息到指定交换机/队列
    //注解：这里不需要队列名：1.若是DIRECT交换机，routingKey就是指定交换机名字；2.FANOUT交换机，绑定的全都发；3.TOPIC交换机要验证bindingKey和routingKey匹配
    public boolean basicPublish(String name,String routingKey,BasicProperties basicProperties,byte[] body) throws mqException {
        //先给交换机加上归属的虚拟主机的标识
        String exchangeName=virtualHostName+name;
        //判断routingKey是否合法？
        if (!router.chechRoutingKey(routingKey)){
            throw new mqException("[VirtualHost] 该routingKey不合法:"+routingKey);
        }
        Exchange exchange = memoryDataCenter.selectExchange(exchangeName);
        //判断交换机
        if(exchange==null){
            throw new mqException("[VirtualHost] 该交换机为空:"+exchangeName);
        }
        synchronized (exchangeLocker) {
            synchronized (queueLocker) {
                try {
                    //判断交换机类型，决定如何发送消息
                    if (exchange.getExchangeType() == exchangeType.DIRECT) {
                        //如果是DIRECT类型的交换机，routingKey就是发送的“目标队列名”
                        //直接发送过去即可
                        Message message = Message.messageFactory(body, basicProperties, routingKey);
                        //查询队列，并且直接发送
                        MessageQueue queue = memoryDataCenter.selectQueue(routingKey);
                        memoryDataCenter.sendMessage(queue,message);
                        diskDataCenter.writeMessage(queue,message);
                        System.out.println("[VirtualHost] DIRECT类型交换机发送消息成功:exchangeName:"+exchangeName
                        +",routingKey:"+routingKey);
                        return true;
                    }
                    //FANOUT/TOPIC交换机的发送情况
                    else {
                        if(exchange.getExchangeType()==exchangeType.FANOUT){
                            Message message = Message.messageFactory(body, basicProperties, routingKey);
                            //FANOUT交换机对所有绑定的队列都进行发送操作
                            ConcurrentHashMap<String, Binding> exchangeBinding = memoryDataCenter.getExchangeBinding(exchangeName);
                            for (Map.Entry<String,Binding> entry:exchangeBinding.entrySet()){
                                String key = entry.getKey();
                                MessageQueue queue=memoryDataCenter.selectQueue(key);
                                if(queue==null){
                                    System.out.println("[VirtualHost] 对于FANOUT交换机，该队列不存在，无法发送消息");
                                    continue;
                                }
                                memoryDataCenter.sendMessage(queue,message);
                                if(message.getDurable()) {
                                    diskDataCenter.writeMessage(queue, message);
                                }
                                System.out.println("[VirtualHost] FANOUT类型交换机发送消息成功:exchangeName:"+exchangeName
                                        +",routingKey:"+routingKey);
                            }
                        }else {
                            Message message=Message.messageFactory(body,basicProperties,routingKey);
                            //先找到交换机对应的绑定，TOPIC交换机需要binding中的bingdingKey
                            ConcurrentHashMap<String,Binding> map=memoryDataCenter.getExchangeBinding(exchangeName);
                            for(Map.Entry<String,Binding> entry:map.entrySet()){
                                Binding value = entry.getValue();
                                //TOPIC类型交换机需要进行bindingKey和routingKey的匹配操作
                                if(!router.BindkeyMatchRoutingkey(exchange.getExchangeType(),value,message)){
                                    System.out.println("[VirtualHost] TOPIC类型交换机，匹配字符串失败:routingKey:"+routingKey+
                                    "bindingKey:"+value.getBindingKey());
                                    continue;
                                }
                                //匹配成功，则进入发消息的准备阶段
                                MessageQueue queue=memoryDataCenter.selectQueue(value.getQueueName());
                                if(queue==null){
                                    System.out.println("[VirtualHost] 对于TOPIC交换机，该队列不存在，无法发送消息");
                                    continue;
                                }
                                sendMessage(queue,message);
                            }
                        }
                    }
                }catch (Exception e){
                    System.out.println("[VirtualHost]  消息发送失败");
                    return false;
                }
            }
        }
        return true;
    }

    private void sendMessage(MessageQueue queue, Message message) throws mqException, IOException {
        //TOPIC类型发送消息
        //先存内存，再存硬盘
            memoryDataCenter.sendMessage(queue,message);
            if(message.getDurable()) {
                diskDataCenter.writeMessage(queue, message);
            }
            System.out.println();
            //通知消费者消费TODO
    }
}
