package org.example.messagequeuepromax.mqserver.core;

import org.example.messagequeuepromax.common.exchangeType;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class Exchange {
    //通过交换机的“名字”来区别每一台交换机
    private String name;

    //本次消息队列中使用到三种交换机类型“DIRECT”,"FANOUT","TOPIC",
    //使用枚举类来实现三种交换机
    private exchangeType exchangeType;

    //持久化存储开关->是否在硬盘上备份保存数据
    private boolean durable;

    //自动删除开关，对于交换机，只要没有生产者，就自动删除
    private boolean autoDelete;

    //交换机的“额外属性”
    private Map<String,Object> arguments=new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public exchangeType getExchangeType() {
        return exchangeType;
    }

    public void setExchangeType(exchangeType exchangeType) {
        this.exchangeType = exchangeType;
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


    /**
     * 另一套set/get Arguments的方法：
     * 因为数据库中存储的是Json,因此要转化为String去存储
     */
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
