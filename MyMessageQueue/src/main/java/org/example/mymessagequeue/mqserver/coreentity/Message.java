package org.example.mymessagequeue.mqserver.coreentity;

import lombok.Data;

import java.io.Serializable;
import java.util.UUID;

/**
 *  “消息”实现Serializable接口，从而数据可以“序列化”
 */
@Data
public class Message implements Serializable {
    //基本属性类的对象
    private BasicProperties properties=new BasicProperties();
    //消息正文
    //为什么是byte[]数组？“消息”通过网络传递和“写入文件持久化”需要转换为二进制形式
    private byte[] body;

    //[begin,end]前闭后开区间
    //消息数据开头离文件开头位置的偏移量
    private int offsetBegin=0;
    //消息数据末尾离文件末尾位置的便宜量
    private int offsetEnd=0;
    //删除标识，用于执行“逻辑删除”数据，0x1数据有效。0x0数据无效
    private byte isValid=0x1;

    //补充的方法：
    //快速获取ID
    public String getId(){
        return properties.getId();
    }
    //快速设置ID
    public void setId(){
        properties.setId("M-"+UUID.randomUUID());
    }
    //快速获取routingKey
    public String getRoutingKey(){
        return properties.getRoutingKey();
    }
    //快速设置routingKey
    public void setRoutingKey(String routingKey){
        properties.setRoutingKey(routingKey);
    }
    //快速获取持久化信息
    public Boolean getDurable(){
        return properties.isDurable();
    }
    //快速设置持久化信息
    public void setDurable(boolean durable){
        properties.setDurable(durable);
    }

    //创建的工厂方法，封装Message的创建
    //Message创建=基本属性类+额外属性
    public Message FactoryMessage(byte[] body,BasicProperties properties,String routingKey){
        Message message=new Message();
        //若参数属性类不为空，覆盖原本属性类
        if(properties!=null){
            message.setProperties(properties);
        }
        message.setId();
        message.setBody(body);
        //若routingKey不为空，覆盖原本
        if(routingKey!=null){
            message.setRoutingKey(routingKey);
        }
        //这里设置的都是“核心属性”，关于offset，删除符号等到“持久化阶段”再设置
        return message;
    }


}
