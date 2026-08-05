package org.example.mymessagequeue.mqserver.coreentity;

import lombok.Data;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class Exchange {
    //唯一，作为“标识”区分交换机
    private String name;
    //使用枚举类，标识三种交换机类型
    private exchangetype exchangeType=exchangetype.DIRECT;
    //持久化开关，服务重启后，该“交换机”是否还在
    private boolean durable=false;
    //自动删除开关，“交换机”与“队列”有绑定关系，用于当“绑定的队列”全删除，是否保留该“交换机”
    private boolean autoDelete=false;
    //额外参数选项，“交换机”的其他参数
    private Map<String, Object> arguments=new HashMap<>();

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public exchangetype getExchageType() {
        return exchangeType;
    }

    public void setExchageType(exchangetype exchageType) {
        this.exchangeType = exchageType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    //

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
