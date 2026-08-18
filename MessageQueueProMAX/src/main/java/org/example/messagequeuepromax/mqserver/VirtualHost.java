package org.example.messagequeuepromax.mqserver;

import org.example.messagequeuepromax.common.*;
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
    //三个锁对象，涉及创建，删除操作，多线程操作可能出现“线程安全”问题
    private Object exchangeLocker=new Object();
    private Object queueLocker=new Object();
    private Object userLocker=new Object();
    //生产者追踪表：channelId -> 该 channel 发布过消息的交换机集合（存“原始”交换机名，不带虚拟主机前缀）
    //用于交换机 autoDelete：断开的 channel 若是某 autoDelete 交换机的最后一个生产者，则自动删除该交换机
    //注：链表本身非线程安全，读写均需 synchronized(链表对象) 保护
    private ConcurrentHashMap<String, LinkedList<String>> producerExchangeMap=new ConcurrentHashMap<>();

    //消费者追踪表：防止造成“还未添加消息的队列被误删除”
    //key：channelId，消费（订阅）过消息的信道，value：queueName，本次订阅涉及的queue
    // 注：链表本身非线程安全，读写均需synchronized（链表对象）保护
    private ConcurrentHashMap<String,LinkedList<String>> consumerQueueMap=new ConcurrentHashMap<>();


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
        //判断要创建的队列是否存在（必须查队列表 selectQueue，而不是绑定表 getExchangeBinding，
        //否则已存在的队列会被重复 new 出来覆盖，导致已注册的消费者全部丢失）
        if(memoryDataCenter.selectQueue(queueName)!=null){
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
        synchronized (queueLocker){
        //先判断删除队列是否存在
        if(memoryDataCenter.selectQueue(queueName)==null){
            throw new mqException("[VirtualHost] 要删除的队列不存在，queueName:"+queueName);
        }
        //进行删除操作
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
        String exchangeName=virtualHostName+binding.getExchangeName();
        String queueName=virtualHostName+binding.getQueueName();
        //判断绑定是否存在
        Binding orderBinding=memoryDataCenter.getUniqueBinding(exchangeName, queueName);
        if(orderBinding==null){
            throw new mqException("[VirtualHost] 该绑定不存在:exchangeName:"+exchangeName
            +",queueName:"+queueName+",bindingKey:"+binding.getBindingKey());
        }
        //如果交换机/队列先被删除了，会先解绑删除，因此这里不会出现交换机/队列为空删除失败的原因
        //直接进行删除
        synchronized (exchangeLocker){
            synchronized (queueLocker){
                try{
                    binding.setExchangeName(exchangeName);
                    binding.setQueueName(queueName);
                    memoryDataCenter.deleteBinding(binding);
                    diskDataCenter.deleteBinding(binding);
                    System.out.println("[VirtualHost] 绑定删除成功:exchangeName:"+exchangeName
                            +",queueName:"+queueName+",bindingKey:"+binding.getBindingKey());
                    return true;
                }catch (Exception e){
                    System.out.println("[VirtualHost] 该绑定删除失败:exchangeName:"+exchangeName
                            +",queueName:"+queueName+",bindingKey:"+binding.getBindingKey());;
                    return false;
                }
            }
        }
    }

    //7.发送消息到指定交换机/队列
    //注解：这里不需要队列名：1.若是DIRECT交换机，routingKey就是指定交换机名字；2.FANOUT交换机，绑定的全都发；3.TOPIC交换机要验证bindingKey和routingKey匹配
    //保留原签名，内部委托给带 channelId 的重载（channelId 传 null 表示不经网络直调，不做生产者登记），不破坏既有调用方（如测试代码）
    public boolean basicPublish(String name,String routingKey,BasicProperties basicProperties,byte[] body) throws mqException {
        return basicPublish(null,name,routingKey,basicProperties,body);
    }

    //带 channelId 的重载：channelId 用于登记“生产者 channel -> 发布过的交换机”关联，供交换机 autoDelete 使用
    public boolean basicPublish(String channelId,String name,String routingKey,BasicProperties basicProperties,byte[] body) throws mqException {
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
        //交换机存在，登记生产者关联（登记“原始”交换机名，便于删除时直接复用 exchangeDelete(name)）
        //链表非线程安全，需同步保护；先判存在再添加，避免同一 channel 重复发布时链表堆积重复项
        if(channelId!=null){
            LinkedList<String> exchangeNames = producerExchangeMap.computeIfAbsent(channelId, k -> new LinkedList<>());
            synchronized (exchangeNames){
                if(!exchangeNames.contains(name)){
                    exchangeNames.add(name);
                }
            }
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
                        sendMessage(queue,message);
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
                                sendMessage(queue,message);
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
                    System.out.println("[VirtualHost]  消息发送失败"+e.getMessage());
                    return false;
                }
            }
        }
        return true;
    }

    private void sendMessage(MessageQueue queue, Message message) throws mqException, IOException, InterruptedException {
        //TOPIC类型发送消息
        //先存内存，再存硬盘
            memoryDataCenter.sendMessage(queue,message);
            if(message.getDurable()) {
                diskDataCenter.writeMessage(queue, message);
            }
            System.out.println();
            //通知消费者消费
        //三种交换机类型都要调用本方法（sendMessage），因为发送完消息，还要即使通知消费者来消费
        ConsumerManager consumerManager=new ConsumerManager(this);
        consumerManager.notifyConsumer(queue.getName());
    }

    //8.订阅消息
    //参数都是构造消费者对象的基本参数
    public boolean basicSubscribe(String qname,String consumerTag, boolean autoAck, Consumer consumer){
        String queueName=virtualHostName+qname;
        synchronized (queueLocker) {
            ConsumerManager consumerManager = new ConsumerManager(this);
            //添加到指定队列的消费者集合
            try {
                consumerManager.addConsumer(queueName,consumerTag,autoAck,consumer);

                //规则：consumerTag==channelId，可以不用重载方法
                //添加本次订阅的channel信道-队列进入追踪表，不存在则创建
                if(consumerTag!=null){
                    LinkedList<String> queueNames=consumerQueueMap.computeIfAbsent(consumerTag,k->new LinkedList<>());
                    synchronized (queueNames){
                        //判重和添加必须用同一个名字体系（qname 原始名），否则判重永远不命中，重复订阅会堆积重复项
                        if(!queueNames.contains(qname)){
                            queueNames.add(qname);
                        }
                    }
                }

                System.out.println("[VirtualHost] 订阅成功,queueName:"+queueName+",consumerTag:"+consumerTag);
                return true;
            } catch (mqException e) {
                System.out.println("[VirtualHost] 订阅失败");
                return false;
            }
        }
    }

    //9.手动应答，消息处理状态
    public boolean basicAck(MessageQueue queue,Message message){
        //检查队列
        if(queue==null){
            System.out.println("[VirtualHost] 队列为空");
            return false;
        }
        //检查消息
        if(message==null){
            System.out.println("[VirtualHost] 消息为空");
            return false;
        }
        //进行手动应答
        //删除消息：硬盘+内存+未确认消息+消息集合
        try {
            memoryDataCenter.deleteMessageById(message.getMessageId());
            memoryDataCenter.deleteUnAckMessage(queue.getName(),message.getMessageId());
            if(message.getDurable()){
                diskDataCenter.deleteMessage(queue,message);
            }
            System.out.println("[VirtualHost] 手动应答成功:queueName:"+queue.getName()+",messageId:"+message.getMessageId());
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] 手动应答失败:queueName:"+queue.getName()+",messageId:"+message.getMessageId());
            return false;
        }
    }

    //10.生产者连接断开后的交换机自动删除检查
    //该 channel 若是某 autoDelete 交换机的“最后一个生产者”，则自动删除该交换机（复用 exchangeDelete，自带解绑+内存硬盘删除+锁保护）
    public void onProducerDisconnect(String channelId){
        if(channelId==null){
            return;
        }
        //取出并移除该 channel 发布过的交换机集合
        LinkedList<String> exchangeNames = producerExchangeMap.remove(channelId);
        if(exchangeNames==null || exchangeNames.isEmpty()){
            return;
        }
        for(String name:exchangeNames){
            try {
                Exchange exchange=memoryDataCenter.selectExchange(virtualHostName+name);
                //交换机已不存在（被手动删除等），或未开启自动删除，跳过
                if(exchange==null || !exchange.isAutoDelete()){
                    continue;
                }
                //检查是否还有其他生产者 channel 发布过该交换机，有则不能删除
                boolean hasOtherProducer=false;
                for(LinkedList<String> list:producerExchangeMap.values()){
                    synchronized (list){
                        if(list.contains(name)){
                            hasOtherProducer=true;
                            break;
                        }
                    }
                }
                if(hasOtherProducer){
                    continue;
                }
                //最后一个生产者已断开，执行自动删除（name 是“原始”交换机名，exchangeDelete 内部会补虚拟主机前缀）
                exchangeDelete(name);
                System.out.println("[VirtualHost] 交换机自动删除成功(无生产者连接),exchangeName:"+virtualHostName+name);
            }catch (mqException e){
                System.out.println("[VirtualHost] 交换机自动删除失败,exchangeName:"+virtualHostName+name);
            }
        }
    }

    //11.消费者断开连接后，检查自动删除队列
    public void onConsumerDisconnect(String channelId){

        synchronized (queueLocker){
        if(channelId==null){
            return;
        }
        //先移除本“追踪表”中的队列（与存储队列没有关系，内存+硬盘的并未因这步操作而删除）
            LinkedList<String> queues = consumerQueueMap.remove(channelId);
        if(queues==null || queues.size()==0){
            return;
        }

        //检查本次channel删除后，是否有“消费标识”的队列中订阅消费者集合为空
            for (String queueName:queues){
            // 检查本次channel删除后，是否有队列中订阅消费者集合为空
            //追踪表存的是“原始”队列名，而 queueMap 的 key 带虚拟主机前缀，查询时需补前缀才能命中
            MessageQueue queue = memoryDataCenter.selectQueue(virtualHostName+queueName);
            //遍历查询是否打开“自动删除”/已经被删除了
            //检查“已删除”/“未开启自动删除”的队列，则不用管
            if (queue == null || !queue.isAutoDelete()) {
                continue;
            }

            //检查这些队列是否消费者订阅集合为空
                boolean ok = memoryDataCenter.ifEmptyQueue(queue);
            //如果为空，则继续检查队列中是否还有消息：若有消息则不删除；若有的是“未确定消息”则删除
                if(ok){
                    //判断队列中有无消息
                    boolean messageOn=memoryDataCenter.ifEmptyMessageQueue(queue);
                    //如果有消息，则不删除
                    if(messageOn){
                        continue;
                    }
                }else {
                    //若还有消费者，则不删除
                    continue;
                }
            //什么都没有，直接删除
            try {
                //删除必须传“原始”队列名：queueDelete 内部会补前缀，若传 queue.getName()（已带前缀）会双重前缀导致删除失败
                queueDelete(queueName);
                System.out.println("[VirtualHost] 队列自动删除成功，queueName:" + queue.getName());
            } catch (mqException e) {
                System.out.println("[VirtualHost] 队列自动删除失败，queueName:" + queue.getName());
            }
        }
    }
    }

    //12.用户登录
    public boolean login(String userName,String password){
        //空判：凭证为null直接失败（否则userMap.get(null)会抛NPE，且NPE会让客户端收不到响应而卡死）
        if(userName==null || password==null){
            System.out.println("[VirtualHost] 登录失败:用户名或密码为空");
            return false;
        }
        //根据“用户名”（主键）查询用户
        UserInfo user = memoryDataCenter.getUser(userName);
        //判断用户是否存在
        //注：对外统一返回false，不区分“用户不存在”和“密码错误”，避免被用于探测有效用户名；区分信息只进日志
        if(user==null){
            System.out.println("[VirtualHost] 用户不存在:"+userName);
            return false;
        }
        //匹配参数
        if(Md5Utils.verify(password,user.getPassword())){
            System.out.println("[VirtualHost] 用户匹配成功，登录成功，欢迎用户:"+userName);
            return true;
        }
        //匹配参数失败
        System.out.println("[VirtualHost] 密码错误，用户匹配失败:"+userName);
        return false;
    }

    //13.用户注册
    public boolean register(String userName,String password) {
        synchronized (userLocker) {
            //先检查数据库中是否含有该用户
            UserInfo user = memoryDataCenter.getUser(userName);
            if (user != null) {
                System.out.println("[VirtualHost] 账户已存在！注册失败");
                return false;
            }
            //不存在该用户则进行注册:内存+硬盘
            UserInfo userInfo = new UserInfo();
            userInfo.setUserName(userName);
            userInfo.setPassword(Md5Utils.encrytion(password));
            memoryDataCenter.insertUser(userInfo);
            diskDataCenter.insertUser(userInfo);
            System.out.println("[VirtualHost] 用户注册成功，欢迎用户:"+userName);
        }
        return true;
    }
}
