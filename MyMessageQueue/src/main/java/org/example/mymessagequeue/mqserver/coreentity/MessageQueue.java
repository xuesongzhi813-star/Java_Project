package org.example.mymessagequeue.mqserver.coreentity;

import lombok.Data;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Data
public class MessageQueue {
    //唯一，用于标识“队列”
    private String name;
    //持久化开关，服务重启后，该“队列”是否还存在
    private boolean durable;
    //指定对应开关，开启后，该队列只能被一个”消费者“使用（其他消费者用不了），当该“消费者”关闭后，队列删除
    private boolean exclusive;
    //自动删除开关，“队列”与“消费者”相关，当没有“消费者”时，队列自动删除开关
    private boolean autoDelete;
    //额外属性
    private Map<String, Object> arguments=new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    /**
     * 改造arguments的setter和getter方法：
     * setter：从数据库中获取到String格式（也可以看作是JSON字符串），转换成Map键值对
     * getter：获取到并且将Map键值对转化为（本质是JSON字符串）String格式“插入数据表”
     * 注意：JSON转换类型：若目标类为简单类，直接写类名即可；
     * 若目标类为复杂类，使用匿名内部类new TypeReference
     */

    public String getArguments() {
        //使用objectmapper调用json的转换
        ObjectMapper objectMapper=new ObjectMapper();
        return objectMapper.writeValueAsString(arguments);
    }

    public void setArguments(String arguments) {
        //使用objectmapper调用向Map键值对转换
        ObjectMapper objectMapper=new ObjectMapper();
        Map<String, Object> stringObjectsMap = objectMapper.readValue(arguments, new TypeReference<Map<String, Object>>() {
        });
        this.arguments=stringObjectsMap;
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
