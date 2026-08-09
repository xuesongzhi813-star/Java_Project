package org.example.mymessagequeue.mqserver.datacenter;

import org.example.mymessagequeue.common.mqException;
import org.example.mymessagequeue.mqserver.coreentity.Binding;
import org.example.mymessagequeue.mqserver.coreentity.Exchange;
import org.example.mymessagequeue.mqserver.coreentity.Message;
import org.example.mymessagequeue.mqserver.coreentity.MessageQueue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理操作内存上的数据部分
 */
public class MemoryDataCenter {
    //交换机存储数据结构，key对应exchangeName(主键锁定),value对应exchange对象
    private ConcurrentHashMap<String, Exchange> exchangeMap=new ConcurrentHashMap<>();
    //队列存储数据结构，key对应queueName（主键锁定），value对应queue对象
    private ConcurrentHashMap<String, MessageQueue> queueMap=new ConcurrentHashMap<>();
    //绑定采取“嵌套式哈希表存储”，原因：绑定是exchangeName与queueName联合产生，需要两个主键确认一个！！
    //第一层：key对应exchangeName，value对应哈希表（包含queueName最终两者对应确认一个绑定）
    //第二层：key对应queueName，value对应binding对象
    private ConcurrentHashMap<String,ConcurrentHashMap<String, Binding>> bindingMap=new ConcurrentHashMap<>();
    /**
     * 这里只要是“消息”，就依赖于“队列”存储（在存储结构中）
     */
    //消息采取“多哈希，多含义”存储
    //第一个哈希表，key对应“消息id”，value对应message对象
    private ConcurrentHashMap<String, Message> correspondingMessageMap=new ConcurrentHashMap<>();
    //第二个哈希表，key对应“队列名”，value对应“属于这个队列的message对象集合（用LinkedList存储）”
    private ConcurrentHashMap<String, LinkedList<Message>> queueBelongMessagesMap=new ConcurrentHashMap<>();
    //未确定消息第一个key对应“queueName”，第二个key对应“messageId”
    private ConcurrentHashMap<String,ConcurrentHashMap<String,Message>> waitAckMessageMap=new ConcurrentHashMap<>();

    /**
     * 关于“交换机”在内存上的操作方法：增，删，改
     */
    //“交换机”增加，目标：存储在哈希表中
    public void insertExchange(Exchange exchange){
        if(exchange==null){
            return;
        }
        exchangeMap.put(exchange.getName(),exchange);
        System.out.println("[MemoryDataCenter] 交换机"+exchange.getName()+"插入成功");
    }

    //“交换机”删除，目标：在哈希表中删除
    public void deleteExchange(String exchangeName){
        exchangeMap.remove(exchangeName);
        System.out.println("[MemoryDataCenter] 交换机"+exchangeName+"删除成功");
    }

    //“交换机”查询，目标：在哈希表中查询出指定的交换机
    public Exchange getExchange(String exchangeName){
       return exchangeMap.get(exchangeName);
    }

    /**
     * 关于“队列”在内存上的操作方法：增，删，改
     */
    //“队列”增加
    public void insertQueue(MessageQueue queue){
        if(queue==null){return;}
        queueMap.put(queue.getName(),queue);
        System.out.println("[MemoryDataCenter] 队列"+queue.getName()+"插入成功");
    }

    //“队列”删除
    public void deleteQueue(String queueName){
        queueMap.remove(queueName);
        System.out.println("[MemoryDataCenter] 队列"+queueName+"删除成功");
    }

    //查询指定”队列“
    public MessageQueue getQueue(String queueName){
        return queueMap.get(queueName);
    }

    /**
     * 关于”绑定“在内存上的操作方法：增，删，改
     */
    //“绑定”的增加
    public void insertBinding(Binding binding) throws mqException {
        // 先根据exchangeName查询是否存在“绑定”依附的哈希表，不存在则创建，存在则插入
        ConcurrentHashMap<String,Binding> bindsingMap=bindingMap.computeIfAbsent(binding.getExchangeName(),
                k->new ConcurrentHashMap<>());
        synchronized (bindsingMap) {
            //多线程同步执行“查询，插入”可能造成多次插入（错误），抛出异常
            //通过第一层exchangeName找到第二层，若queueName也存在，则说明一二层联通，“绑定”已经存在！！！，不能增加
            if (bindsingMap.get(binding.getMessageQueueName()) != null) {
                throw new mqException("[MemoryDataCenter] 绑定已经存在！exchangeName:" + binding.getExchangeName()
                        + ",queueName:" + binding.getMessageQueueName());
            }
            //绑定添加
            bindsingMap.put(binding.getMessageQueueName(), binding);
        }
        System.out.println("[MemoryDataCenter] 绑定添加成功 exchangeName:"+binding.getExchangeName()
                +",queueName:"+binding.getMessageQueueName());
    }

    //“绑定”的获取
    //根据指定获取“唯一”的绑定
    public Binding getUniqueBinding(String exchangeName,String queueName){
        //先查出“逻辑上”的第二层“绑定”依附层
        ConcurrentHashMap<String, Binding> stringBindingConcurrentHashMap = bindingMap.get(exchangeName);
        if(stringBindingConcurrentHashMap==null){
            return null;
        }
        //第二层查询
        Binding binding = stringBindingConcurrentHashMap.get(queueName);
        return binding;
    }

    //根据exchangeName获取“绑定”集合（归属于指定exchange的）
    public ConcurrentHashMap<String, Binding> getListBinding(String exchangeName){
        ConcurrentHashMap<String, Binding> stringBindingConcurrentHashMap = bindingMap.get(exchangeName);
        if(stringBindingConcurrentHashMap==null){
            return null;
        }
        //返回链表时使用
//        LinkedList<Binding> bindings= (LinkedList<Binding>) stringBindingConcurrentHashMap.values();
        return stringBindingConcurrentHashMap;
    }

    //根据queueName获取所有关联的绑定集合（遍历所有交换机下的绑定）
    public List<Binding> getBindingsByQueueName(String queueName){
        List<Binding> result = new LinkedList<>();
        for (ConcurrentHashMap<String, Binding> innerMap : bindingMap.values()) {
            Binding binding = innerMap.get(queueName);
            if (binding != null) {
                result.add(binding);
            }
        }
        return result;
    }

    //删除绑定
    public void deleteBinding(Binding binding) throws mqException {
        //先通过exchangeName查询，依附队列存在的绑定是否存在
        ConcurrentHashMap<String, Binding> stringBindingConcurrentHashMap = bindingMap.get(binding.getExchangeName());
        //如果不存在，报错（根本无这个绑定）
        if(stringBindingConcurrentHashMap==null){
            throw new mqException("[MemoryDataCenter] 绑定不存在 ! exchangeName:"+binding.getExchangeName()
            +",queueName:"+binding.getExchangeName());
        }
        //存在则删除
//        stringBindingConcurrentHashMap.remove(binding);
        stringBindingConcurrentHashMap.remove(binding.getMessageQueueName());
//        bindingMap.remove(binding.getExchangeName(),stringBindingConcurrentHashMap);
        System.out.println("[MemoryDataCenter] 绑定已删除 exchangeName:"+binding.getExchangeName()
                +",queueName:"+binding.getMessageQueueName());
    }

    /**
     * 实现消息管理（关于id-message哈希表）的操作
     */
    //添加消息
    public void addMessage(Message message){
        correspondingMessageMap.put(message.getId(), message);
        System.out.println("[MemoryDataCenter] 消息添加成功:"+message.getId());
    }
    //根据id查消息
    public Message getById(String messageId){
        return correspondingMessageMap.get(messageId);
    }
    //根据id删除消息
    public void deleteById(String messageId){
        correspondingMessageMap.remove(messageId);
        System.out.println("[MemoryDataCenter] 删除消息成功:"+messageId);
    }

    /**
     * 实现消息管理（关于queue-message依附存储哈希表）的操作
     */
    //发送信息到指定队列
    public void sendMessage(MessageQueue queue,Message message){
        //先查询是否有指定队列存在(依附于队列的message链表集合)，无则创建，作为第一条数据插入
        LinkedList<Message> messages = queueBelongMessagesMap.computeIfAbsent(queue.getName(), k -> new LinkedList<>());
        //尾插数据，多线程可能导致插入顺序有误
        synchronized (messages) {
            messages.add(message);
        }
        //向总表插入
        queueBelongMessagesMap.put(queue.getName(),messages);
        System.out.println("[MemoryDataCenter] 消息插入队列成功:"+message.getId()+",queue:"+queue.getName());
    }

    //从队列中取出消息(指定队列的集合中取出元素)
    public Message pollMessage(MessageQueue queue) {
        //先获得集合
        LinkedList<Message> messages = queueBelongMessagesMap.get(queue.getName());
        //判断为空
        if (messages == null) {
            return null;
        }
        synchronized (messages) {
            //判断不含元素
            if (messages.size() == 0) {
                return null;
            }
            //取出消息，头删(队列特性：先进先出)，多个线程一起取，可能造成线程安全
            Message remove = messages.remove(0);
            System.out.println("[MemoryDataCenter] 取出消息:"+remove.getId()+",queue:"+queue.getName());
            return remove;
        }
    }

    //获取指定队列中的消息总数
    public int totalMessage(MessageQueue queue){
        //获取集合
        LinkedList<Message> messages=queueBelongMessagesMap.get(queue.getName());
        if(messages==null){
            return 0;
        }
        synchronized (messages) {
            return messages.size();
        }
    }

    /**
     * 对未确定数据的管理:
     */
    //添加“未确定数据”
    public void addUnAckMessage(String queueName,Message message){
        ConcurrentHashMap<String, Message> stringMessageConcurrentHashMap =
                waitAckMessageMap.computeIfAbsent(queueName, k -> new ConcurrentHashMap<>());
        synchronized (stringMessageConcurrentHashMap){
            stringMessageConcurrentHashMap.put(message.getId(),message);
        }
        System.out.println("[MemoryDataCenter] 添加“未确定数据”成功:"+message.getId()
                +",queueName:"+queueName);
    }

    //删除“未确定数据”
    public void deleteUnAcMessage(String queueName,String messageId){
        ConcurrentHashMap<String, Message> stringMessageConcurrentHashMap = waitAckMessageMap.get(queueName);
        if(stringMessageConcurrentHashMap==null){
            return;
        }
        if(stringMessageConcurrentHashMap.get(messageId)!=null) {
            synchronized (stringMessageConcurrentHashMap) {
                stringMessageConcurrentHashMap.remove(messageId);
                System.out.println("删除未确认数据成功:"+messageId);
            }
        }else {
            return;
        }
    }

    //获取指定未确定数据
    public Message getOrderUnAckMessage(String queueName,String messageId){
        //先查询
        ConcurrentHashMap<String, Message> stringMessageConcurrentHashMap = waitAckMessageMap.get(queueName);
        if (stringMessageConcurrentHashMap==null){
            return null;
        }
        synchronized (stringMessageConcurrentHashMap){
            if(stringMessageConcurrentHashMap.get(messageId)!=null){
                return stringMessageConcurrentHashMap.get(messageId);
            }
            return null;
        }
    }

    /**
     * 恢复硬盘中所有存储数据（在服务器重启后，恢复到内存）:通过硬盘操作类实现
     */
    public void recovery(DiskDataCenter diskDataCenter) throws mqException, IOException, ClassNotFoundException {
        //1.恢复交换机数据
        exchangeMap.clear();
        List<Exchange> exchanges = diskDataCenter.selectAllExchange();
        for (Exchange exchange:exchanges){
            exchangeMap.put(exchange.getName(),exchange);
        }

        //2.恢复队列数据
        queueMap.clear();
        List<MessageQueue> queues=diskDataCenter.selectAllQueue();
        for (MessageQueue queue:queues){
            queueMap.put(queue.getName(),queue);
        }

        //3.恢复绑定数据
        bindingMap.clear();
        List<Binding> bindings = diskDataCenter.selectAllBinding();
        for (Binding binding:bindings){
            //先查依附存储的哈希表，再插入
            ConcurrentHashMap<String, Binding> map = bindingMap
                    .computeIfAbsent(binding.getExchangeName(), k -> new ConcurrentHashMap<>());
            map.put(binding.getMessageQueueName(),binding);
            bindingMap.put(binding.getExchangeName(),map);
        }

        //恢复消息
        queueBelongMessagesMap.clear();
        correspondingMessageMap.clear();
        //先遍历所有队列，找到所有对应消息
        for (MessageQueue queue:queues) {
            LinkedList<Message> messages = diskDataCenter.loadAllMessage(queue.getName());
            //先恢复存储的哈希表
            queueBelongMessagesMap.put(queue.getName(),messages);
            //再恢复id哈希表
            for (Message message:messages){
                correspondingMessageMap.put(message.getId(),message);
            }
        }
    }
}
