package org.example.mymessagequeue.mqserver;

import org.example.mymessagequeue.common.Consumer;
import org.example.mymessagequeue.common.ConsumerEnv;
import org.example.mymessagequeue.common.mqException;
import org.example.mymessagequeue.mqserver.coreentity.*;
import org.example.mymessagequeue.mqserver.datacenter.DiskDataCenter;
import org.example.mymessagequeue.mqserver.datacenter.MemoryDataCenter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  虚拟主机：对业务逻辑的整合，统一处理异常
 *  逻辑隔离不同业务的交换机等数据
 *  管理数据+API调用
 */
public class VirtualHost {
    private String virtualHostName;
    private DiskDataCenter diskDataCenter=new DiskDataCenter();
    private MemoryDataCenter memoryDataCenter=new MemoryDataCenter();
    private Router router=new Router();
    ConsumerManager consumerManager=new ConsumerManager(this);

    public String getVirtualHostName() {
        return virtualHostName;
    }

    public DiskDataCenter getDiskDataCenter() {
        return diskDataCenter;
    }

    public MemoryDataCenter getMemoryDataCenter() {
        return memoryDataCenter;
    }

    //锁对象
    private Object exchangeLocker=new Object();
    private Object queueLocker=new Object();

    //构造方法：初始化虚拟主机+初始化硬盘，内存
    public VirtualHost(String virtualHostName){
        this.virtualHostName=virtualHostName;
        //初始化硬盘，建库建表
        diskDataCenter.init();
        //恢复内存数据
        try {
            memoryDataCenter.recovery(diskDataCenter);
        } catch (mqException | IOException | ClassNotFoundException e) {
            System.out.println("[VirtualHostName] 恢复内存数据失败:"+virtualHostName);
            throw new RuntimeException(e);
        }
    }

    /**
     * 核心上层调用API实现：
     */
    //1.创建交换机
    public boolean exchangeDeclare(String exchangeName, exchangetype type, boolean durable, boolean autoDelete, Map<String,Object> arguments) throws mqException {
        String name=virtualHostName+exchangeName;
        synchronized (exchangeLocker) {
            try {
                //判断交换机是否存在
                Exchange existExchange = memoryDataCenter.getExchange(name);
                if (existExchange != null) {
                    System.out.println("[VirtualHostName] 该交换机已经存在：" + name);
                    return true;
                }
                //构造交换机对象
                Exchange exchange = new Exchange();
                exchange.setName(name);
                exchange.setExchageType(type);
                exchange.setDurable(durable);
                exchange.setAutoDelete(autoDelete);
                exchange.setArguments(arguments);
                //判断是否支持持久化该交换机，写入硬盘
                if (durable) {
                    diskDataCenter.insertExchange(exchange);
                }
                //写入内存
                memoryDataCenter.insertExchange(exchange);
                System.out.println("[VirtualHostName] 交换机创建成功:" + name);
            } catch (Exception e) {
                System.out.println("[VirtualHostName] 交换机创建异常:" + name);
                return false;
            }
        }
        return true;
    }

    //2.删除交换机
    public boolean exchangeDelete(String exchangeName){
        String name=virtualHostName+exchangeName;
        synchronized (exchangeLocker) {
            try {
                //判断交换机是否存在
                Exchange exchange = memoryDataCenter.getExchange(name);
                if (exchange == null) {
                    throw new mqException("[VirtualHostName] 交换机不存在:" + name);
                }
                //先获取该交换机下所有绑定，逐一解绑（先删内存绑定关系，再删硬盘绑定记录）
                ConcurrentHashMap<String, Binding> listBinding = memoryDataCenter.getListBinding(name);
                if (listBinding != null && !listBinding.isEmpty()) {
                    for (Binding binding : listBinding.values()) {
                        memoryDataCenter.deleteBinding(binding);
                        diskDataCenter.deleteBinding(binding);
                    }
                }
                //再删除交换机（先删硬盘，再删内存）
                diskDataCenter.deleteExchange(name);
                memoryDataCenter.deleteExchange(name);
                System.out.println("[VirtualHostName] 交换机删除成功:" + name);
            } catch (Exception e) {
                System.out.println("[VirtualHostName] 交换机删除失败:" + name + ", 原因:" + e.getMessage());
                return false;
            }
        }
        return true;
    }

    //3.创建队列
    public boolean queueDeclare(String queueName,boolean durable,boolean exclusive,boolean autoDelete,Map<String,Object> arguments){
        String name=virtualHostName+queueName;
        synchronized (queueLocker) {
            try {
                //先判断队列是否已经存在
                MessageQueue existQueue = memoryDataCenter.getQueue(name);
                if (existQueue != null) {
                    System.out.println("[VirtualHostName] 队列已经存在:" + name);
                    return true;
                }
                //构造队列
                MessageQueue queue = new MessageQueue();
                queue.setName(name);
                queue.setDurable(durable);
                queue.setExclusive(exclusive);
                queue.setAutoDelete(autoDelete);
                queue.setArguments(arguments);
                //判断是否可持久化
                if (durable) {
                    diskDataCenter.insertQueue(queue);
                }
                memoryDataCenter.insertQueue(queue);
            } catch (Exception e) {
                System.out.println("[VirtualHostName] 队列创建失败:" + name);
                return false;
            }
        }
        return true;
    }

    //4.删除队列
    public boolean queueDelete(String queueName){
        String name=virtualHostName+queueName;
        synchronized (queueLocker) {
            try {
                //判断队列是否存在
                MessageQueue existQueue = memoryDataCenter.getQueue(name);
                if (existQueue == null) {
                    throw new mqException("[VirtualHostName] 队列不存在:" + name);
                }
                //先找到该队列关联的所有绑定，逐一解绑（先删内存绑定关系，再删硬盘绑定记录）
                List<Binding> bindings = memoryDataCenter.getBindingsByQueueName(name);
                if (bindings != null && !bindings.isEmpty()) {
                    for (Binding binding : bindings) {
                        memoryDataCenter.deleteBinding(binding);
                        diskDataCenter.deleteBinding(binding);
                    }
                }
                //再删除队列（先删硬盘，再删内存）
                diskDataCenter.deleteQueue(name);
                memoryDataCenter.deleteQueue(name);
                System.out.println("[VirtualHostName] 队列删除成功:" + name);
            } catch (Exception e) {
                System.out.println("[VirtualHostName] 队列删除失败:" + name + ", 原因:" + e.getMessage());
                return false;
            }
        }
        return true;
    }

    //5.创建绑定
    public boolean bindingDeclare(String exchangeName,String queueName,String bindingKey){
        String exchangeN=virtualHostName+exchangeName;
        String queueN=virtualHostName+queueName;
        synchronized (exchangeLocker) {
            synchronized (queueLocker) {
                try {
                    //判断绑定是否存在
                    Binding uniqueBinding = memoryDataCenter.getUniqueBinding(exchangeN, queueName);
                    if (uniqueBinding != null) {
                        System.out.println("[VirtualHostName] 绑定已经存在:" + bindingKey + ",exchangeName:" + exchangeN
                                + ",queueName:" + queueN);
                        return true;
                    }
                    //判断绑定是否合法
                    if (!Router.bindingRouter(bindingKey)) {
                        throw new mqException("[VirtualHostName] 该绑定不合法:" + bindingKey);
                    }
                    //创建绑定
                    Binding binding = new Binding();
                    binding.setExchangeName(exchangeN);
                    binding.setMessageQueueName(queueN);
                    binding.setBindingKey(bindingKey);
                    //判断交换机和队列是否存在
                    if (memoryDataCenter.getExchange(exchangeN) == null) {
                        throw new mqException("[VirtualHostName] 交换机不存在:" + exchangeN);
                    }
                    if (memoryDataCenter.getQueue(queueN) == null) {
                        throw new mqException("[VirtualHostName] 队列不存在:" + queueN);
                    }
                    //插入硬盘，内存
                    diskDataCenter.insertBinding(binding);
                    memoryDataCenter.insertBinding(binding);
                } catch (Exception e) {
                    System.out.println("[VirtualHostName] 绑定创建失败:" + bindingKey + ",exchangeName:" + exchangeN
                            + ",queueName:" + queueN);
                    return false;
                }
            }
        }
        return true;
    }

    //6.删除绑定
    public boolean bindingDelete(Binding binding){
        String exchangeName=virtualHostName+binding.getExchangeName();
        String queueName=virtualHostName+binding.getMessageQueueName();
        synchronized (exchangeLocker) {
            synchronized (queueLocker) {
                try {
                    //检查是否存在
                    Binding uniqueBinding = memoryDataCenter.getUniqueBinding(exchangeName, queueName);
                    if (uniqueBinding == null) {
                        throw new mqException("[VirtualHostName] 绑定不存在:" + binding.getBindingKey() + ",exchangeName:" + exchangeName
                                + ",queueName:" + queueName);
                    }
                    //先解绑（删除内存中的绑定关系），再删除硬盘中的绑定记录
                    memoryDataCenter.deleteBinding(uniqueBinding);
                    diskDataCenter.deleteBinding(uniqueBinding);
                    System.out.println("[VirtualHostName] 绑定删除成功: exchangeName:" + exchangeName + ",queueName:" + queueName);
                    return true;
                } catch (Exception e) {
                    System.out.println("[VirtualHostName] 绑定删除失败:" + e.getMessage());
                    return false;
                }
            }
        }
    }
    //7.发送消息到指定交换机/队列中
    public boolean basicPublish(String exchangeName,String routingKey,BasicProperties properties,byte[] body){
        String name=virtualHostName+exchangeName;
        synchronized (exchangeLocker) {
            synchronized (queueLocker) {
                //消息:先到交换机->队列
                try {
                    //判断RoutingKey合法
                    if (!Router.routingKeyRouter(routingKey)) {
                        throw new mqException("[VirtualHostName] RoutingKey不合法:" + routingKey);
                    }
                    //查询交换机
                    Exchange exchange = memoryDataCenter.getExchange(name);
                    if (exchange == null) {
                        throw new mqException("[VirtualHostName] 交换机不存在:" + name);
                    }
                    //判断交换机类型
                    if (exchange.getExchageType() == exchangetype.DIRECT) {
                        Message message = Message.FactoryMessage(body, properties, routingKey);
                        //查找指定队列
                        String queueName = virtualHostName + routingKey;
                        MessageQueue queue = memoryDataCenter.getQueue(queueName);
                        //此时routingKey就是要发送的队列名，直接发送
                        memoryDataCenter.sendMessage(queue, message);
                    } else {
                        //判断是剩下哪种类型
                        if (exchange.getExchageType() == exchangetype.FANOUT) {
                            //找所有绑定，遍历键值对
                            ConcurrentHashMap<String, Binding> listBinding = memoryDataCenter.getListBinding(name);
                            for (Map.Entry<String, Binding> entry : listBinding.entrySet()) {
                                Binding binding = entry.getValue();
                                //查队列
                                MessageQueue queue = memoryDataCenter.getQueue(binding.getMessageQueueName());
                                if (queue == null) {
                                    System.out.println("[VirtualHostName] 发送消息时，发现队列不存在:" + binding.getMessageQueueName());
                                    continue;
                                }
                                Message message = Message.FactoryMessage(body, properties, routingKey);
                                //判断是否要发送给队列/
                                //FANOUT
                                //TOPIC
                                if (!Router.routeTopic(exchange.getExchageType(), binding, message)) {
                                    continue;
                                }
                                sendMessage(queue, message);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("[VirtualHostName] 消息发送失败");
                    return false;
                }
            }
        }
        return true;
    }

    private void sendMessage(MessageQueue queue, Message message) throws mqException, IOException, InterruptedException {
        if (message.getDurable()){
            diskDataCenter.sendMessage(queue,message);
        }
        memoryDataCenter.sendMessage(queue,message);
        //通知消费者，消费消息了
        consumerManager.notifyConsumer(queue.getName());
    }

    //7.消费消息
    public boolean basicConsume(String counsumerTag,String queueName,boolean autoAck, Consumer consumer){
        queueName=virtualHostName+queueName;
        try {
            //构造消费者对象
            ConsumerEnv consumerEnv=new ConsumerEnv(counsumerTag,queueName,autoAck,consumer);
            //
            consumerManager.addConsumer(counsumerTag,queueName,autoAck,consumer);
            System.out.println("[VirtualHostName] basicConsume成功！");
            return true;
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("[VirtualHostname] basicConsume失败！");
        }
        return false;
    }
}
