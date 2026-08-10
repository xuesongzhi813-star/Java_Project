package org.example.mymessagequeue.mqclient;

import org.example.mymessagequeue.common.*;
import org.example.mymessagequeue.mqserver.coreentity.BasicProperties;
import org.example.mymessagequeue.mqserver.coreentity.exchangetype;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Channel {
    //channel的身份标识
    private String channelId;
    //当前channel属于哪个Connection下
    private Connection connection;
    //存储服务器的响应结果
    //key为rid，value为返回的响应
    private ConcurrentHashMap<String, BasicReturns> returnsMap=new ConcurrentHashMap<>();
    //回调函数
    private Consumer consumer=null;

    public Channel(String channelId,Connection connection){
        this.channelId=channelId;
        this.connection=connection;
    }

    //生成rid的值
    public String genereteRid(){
       return UUID.randomUUID().toString();
    }

    //调用服务器的创建channelAPI，告知服务器创建channel
    public boolean ChcreateChannel() throws IOException {
        //构造请求
        BasicArguments basicArguments=new BasicArguments();
        basicArguments.setRid(genereteRid());
        basicArguments.setChannelId(channelId);
        byte[] payload= BinaryTool.toByte(basicArguments);
        Request request=new Request();
        request.setType(0x1);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送请求
        connection.writeRequest(request);
        //等待响应
        BasicReturns basicReturns =waitResult(basicArguments.getRid());
        return basicReturns.isOk();
    }

    //根据rid（请求与响应的配对标识）等待服务器响应
    private BasicReturns waitResult(String rid) {
        BasicReturns basicReturns;
        //查表和等待必须都在同一个锁内，防止“丢失唤醒”；唤醒后重新查表
        synchronized (this){
            //如果查表没查询到，就阻塞等待响应；被唤醒后重新查表
            while ((basicReturns=returnsMap.get(rid))==null){
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        //得到响应后，从哈希表中删除，不再占用位置
        returnsMap.remove(rid);
            //返回响应
            return basicReturns;
    }

    //channel调用服务器的API，关闭channel
    public boolean closeChannel() throws IOException {
        //构造请求
        BasicArguments basicArguments=new BasicArguments();
        basicArguments.setRid(genereteRid());
        basicArguments.setChannelId(channelId);
        byte[] payload=BinaryTool.toByte(basicArguments);
        Request request=new Request();
        request.setType(0x2);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送并且等待响应
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(basicArguments.getRid());
        return basicReturns.isOk();
    }

    //channel调用服务器API，创建交换机
    public boolean exchangeDeclare(String exchangeName, exchangetype type, boolean durable, boolean autoDelete, Map<String,Object> arguments) throws IOException {
        //构造请求
        ExchangeDeclareArgument argument=new ExchangeDeclareArgument();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setExchangeName(exchangeName);
        argument.setExchangetype(type);
        argument.setDurable(durable);
        argument.setAutoDelete(autoDelete);
        argument.setArguments(arguments);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x3);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送并且等待响应
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(argument.getRid());
        return basicReturns.isOk();
    }

    //channel调用服务器API，删除交换机
    public boolean exchangeDelete(String exchangeName) throws IOException {
        ExchangeDeleteArgument argument=new ExchangeDeleteArgument();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setExchangeName(exchangeName);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x4);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送并且等待响应
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(argument.getRid());
        return basicReturns.isOk();
    }

    //channel调用服务器API，创建队列
    public boolean queueDeclare(String queueName,boolean durable,boolean exclusive,boolean autoDelete,Map<String,Object> arguments) throws IOException {
        //构造请求
        QueueDeclareArgument argument=new QueueDeclareArgument();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setQueueName(queueName);
        argument.setDurable(durable);
        argument.setExclusive(exclusive);
        argument.setAutoDelete(autoDelete);
        argument.setArguments(arguments);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x5);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送并且等待响应
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(argument.getRid());
        return basicReturns.isOk();
    }

    //channel调用服务器API，删除队列
    public boolean queueDelete(String queueName) throws IOException {
        //构造请求
        QueueDeleteArgument argument=new QueueDeleteArgument();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setQueueName(queueName);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x6);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送并且等待响应
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(argument.getRid());
        return basicReturns.isOk();
    }

    //channel调用服务器API，创建绑定
    public boolean BindingDeclare(String exchangeName,String queueName,String bindingKey) throws IOException {
        //构造请求
        BindingDeclareArgument argument=new BindingDeclareArgument();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setExchangeName(exchangeName);
        argument.setQueueName(queueName);
        argument.setBindingKey(bindingKey);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x7);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送并且等待响应
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(argument.getRid());
        return basicReturns.isOk();
    }

    //channel调用服务器API，删除绑定
    public boolean BindingDelete(String exchangeName,String queueName) throws IOException {
        //构造请求
        BindingDeleteArgument argument=new BindingDeleteArgument();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setExchangeName(exchangeName);
        argument.setQueueName(queueName);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x8);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送并且等待响应
        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(argument.getRid());
        return basicReturns.isOk();
    }

    // 发送消息
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) throws IOException {
        BasicPublishArgument arguments = new BasicPublishArgument();
        arguments.setRid(genereteRid());
        arguments.setChannelId(channelId);
        arguments.setExchangeName(exchangeName);
        arguments.setRoutingKey(routingKey);
        arguments.setBasicProperties(basicProperties);
        arguments.setBody(body);
        byte[] payload = BinaryTool.toByte(arguments);

        Request request = new Request();
        request.setType(0x9);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }

    // 订阅消息
    public boolean basicConsume(String queueName, boolean autoAck, Consumer consumer) throws IOException, mqException {
        // 先设置回调.
        if (this.consumer != null) {
            throw new mqException("该 channel 已经设置过消费消息的回调了, 不能重复设置!");
        }
        this.consumer = consumer;

        BasicConsumeArgument arguments = new BasicConsumeArgument();
        arguments.setRid(genereteRid());
        arguments.setChannelId(channelId);
        arguments.setConsumerTag(channelId);  // 此处 consumerTag 也使用 channelId 来表示了.
        arguments.setQueueName(queueName);
        arguments.setAutoAck(autoAck);
        byte[] payload = BinaryTool.toByte(arguments);

        Request request = new Request();
        request.setType(0xa);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }

    // 确认消息
    public boolean basicAck(String queueName, String messageId) throws IOException {
        BasicAckArgument arguments = new BasicAckArgument();
        arguments.setRid(genereteRid());
        arguments.setChannelId(channelId);
        arguments.setQueueName(queueName);
        arguments.setMessageId(messageId);
        byte[] payload = BinaryTool.toByte(arguments);
        Request request = new Request();
        request.setType(0xb);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isOk();
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public ConcurrentHashMap<String, BasicReturns> getReturnsMap() {
        return returnsMap;
    }

    public void setReturnsMap(ConcurrentHashMap<String, BasicReturns> returnsMap) {
        this.returnsMap = returnsMap;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }


    public void putReturns(BasicReturns basicReturns) {
        returnsMap.put(basicReturns.getRid(),basicReturns);
        synchronized (this){
            //当前不知道多少线程等待这个响应
            //先把所有线程唤醒，对应这个响应的就会得到这个响应处理，其他的线程继续等待自己的响应
            notifyAll();
        }
    }
}
