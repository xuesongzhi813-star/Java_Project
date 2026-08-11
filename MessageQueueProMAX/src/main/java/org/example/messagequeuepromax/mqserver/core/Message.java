package org.example.messagequeuepromax.mqserver.core;

import java.io.Serializable;
import java.util.UUID;

/**
 * 自定义消息的格式：基本属性+正文内容
 * 存储在本地文件（二进制格式）：长度+消息对象序列化的数据
 */
public class Message implements Serializable {
    //消息的基本属性，存储为一个对象
    private BasicProperties basicProperties;

    //消息的主体信息，采取二进制存储
    private byte[] body;

    //消息进行“逻辑删除”的标识
    private byte isValid=0x1;

    //消息序列化后存储在文件中的offset标记消息起始
    private int offsetBegin;
    private int offsetEnd;

    /**
     * 消息工厂方法，封装message对象的构造过程，因为涉及basic类的构造
     *body+basicproperties（采取“覆盖”操作），routingKey是决定消息发给哪个队列的关键。也需要覆盖设置
     * 注意！！属性类+routingKey不能为空，因为routingKey是传输标记+routingKey依赖于属性类存在，属性类就不能空
     */
    public Message messageFactory(byte[] body,BasicProperties basicProperties,String routingKey){
        Message message=new Message();
        if(basicProperties!=null){
            //若属性类不为空，直接覆盖原属性
            message.setBasicProperties(basicProperties);
        }
        message.setBody(body);
        //Id的设定采取UUID随机
        message.setMessageId("M-"+ UUID.randomUUID());
        if(routingKey!=null){
            message.setRoutingKey(routingKey);
        }
        return message;
    }

    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getOffsetBegin() {
        return offsetBegin;
    }

    public void setOffsetBegin(int offsetBegin) {
        this.offsetBegin = offsetBegin;
    }

    public byte getIsValid() {
        return isValid;
    }

    public void setIsValid(byte isValid) {
        this.isValid = isValid;
    }

    public int getOffsetEnd() {
        return offsetEnd;
    }

    public void setOffsetEnd(int offsetEnd) {
        this.offsetEnd = offsetEnd;
    }

    /**
     * 构造能取到消息属性的get/set方法（通过basicProperties太麻烦）
     */
    public String getMessageId(){
        return this.basicProperties.getMessageId();
    }

    public boolean getDurable(){
        return this.basicProperties.isDurable();
    }

    public String getroutingKey(){
        return this.basicProperties.getRoutingKey();
    }

    public long getTime(){
        return this.basicProperties.getCurrentTime();
    }

    public void setMessageId(String messageId){
        this.basicProperties.setMessageId(messageId);
    }

    public void setdurable(boolean durable){
        this.basicProperties.setDurable(durable);
    }

    public void setRoutingKey(String routingKey){
        this.basicProperties.setRoutingKey(routingKey);
    }
}
