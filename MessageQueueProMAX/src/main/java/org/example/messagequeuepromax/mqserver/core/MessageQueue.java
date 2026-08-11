package org.example.messagequeuepromax.mqserver.core;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class MessageQueue {
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
}
