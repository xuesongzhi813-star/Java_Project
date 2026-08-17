package org.example.messagequeuepromax.mqserver.core;

import org.example.messagequeuepromax.common.ConsumerEnv;
import org.example.messagequeuepromax.common.mqException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageQueue implements Serializable {
    //通过队列名字来标识唯一的队列
    private String name;

    //独占开关，队列是否只能被一个消费者使用，其他消费者不能使用
    private boolean exclusive;

    //持久化存储开关，是否在硬盘上备份信息
    private boolean durable;

    //自动删除开关，对于“队列”如果消费者不存在，则自动删除队列
    private boolean autoDelete;

    //额外属性配置
    private Map<String,Object> arguments=new HashMap<>();

    //管理订阅本队列的消费者集合
    List<ConsumerEnv> consumerEnvList=new ArrayList<>();

    //订阅本队列的消费者，轮流来取消息消费
    //轮流下标
    private AtomicInteger consumerIndex=new AtomicInteger(0);

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }


    //从数据库中取出JSON，要转为哈希表才能set属性
    public void setArguments(String argument){
        ObjectMapper objectMapper=new ObjectMapper();
        Map<String, Object> stringObjectMap = objectMapper.readValue(argument, new TypeReference<Map<String, Object>>() {
        });
        this.arguments=stringObjectMap;
    }

    //从对象中get哈希表属性，要转换为JSON字符串才能进行插入操作
    public String getArguments(){
        ObjectMapper objectMapper=new ObjectMapper();
        String s = objectMapper.writeValueAsString(arguments);
        return s;
    }

    /**
     * 新增一组便于测试的setter，getter方法
     * 便利在哪？最原始的传递String和Map
     */
    public Object getArguments(Object key) {
        return arguments.get(key);
    }

    public void setArguments(String key,Object value){
        this.arguments.put(key, value);
    }

    public void setArguments(Map<String,Object> arguments) {
        this.arguments=arguments;
    }

    //添加订阅的消费者
    public void addConsumerEnv(ConsumerEnv consumerEnv){
        //检查消费者是否有效
        if(consumerEnv==null){
            System.out.println("[MessageQueue] 该消费者无效，无法添加");
            return;
        }
        consumerEnvList.add(consumerEnv);
        System.out.println("[MessageQueue] 消费者订阅添加成功:consumerTag:"+consumerEnv.getConsumerTag());
    }

    //删除订阅的消费者
    public void deleteConsumerEnv(ConsumerEnv consumerEnv) throws mqException {
        //查找是否存在订阅
        if(!consumerEnvList.contains(consumerEnv)){
            throw new mqException("[MessageQueue] 该消费者订阅不存在");
        }
        //进行删除
        consumerEnvList.remove(consumerEnv);
        System.out.println("[MessageQueue] 消费者订阅删除成功:consumerTag:"+consumerEnv.getConsumerTag());
    }

    //根据 consumerTag（即channelId）移除订阅的消费者：消费者连接断开时清理"死订阅"用
    public boolean deleteConsumerEnvByTag(String consumerTag){
        synchronized (this) {
            Iterator<ConsumerEnv> iterator = consumerEnvList.iterator();
            while (iterator.hasNext()) {
                ConsumerEnv consumerEnv = iterator.next();
                if (consumerTag!=null && consumerTag.equals(consumerEnv.getConsumerTag())) {
                    iterator.remove();
                    System.out.println("[MessageQueue] 消费者订阅清理成功(连接断开):consumerTag:"+consumerTag);
                    return true;
                }
            }
        }
        return false;
    }

    //挑选消费者轮流进行消费消息
    public ConsumerEnv selectConsumer(){
        //先进行查询，订阅集合中有无对象
        if(consumerEnvList.size()==0){
            System.out.println("[MessageQueue] 订阅集合中没有消费者");
            return null;
        }
        //计算取定下标
        int index=consumerIndex.get()%consumerEnvList.size();
        //根据下标获取到本次消费的消费者
        ConsumerEnv consumerEnv = consumerEnvList.get(index);
        //下标自增
        consumerIndex.getAndIncrement();
        return consumerEnv;
    }
}
