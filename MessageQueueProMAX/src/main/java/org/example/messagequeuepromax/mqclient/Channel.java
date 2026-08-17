package org.example.messagequeuepromax.mqclient;

import org.example.messagequeuepromax.common.*;
import org.example.messagequeuepromax.mqserver.core.BasicProperties;
import org.example.messagequeuepromax.mqserver.core.Binding;
import org.example.messagequeuepromax.mqserver.core.Message;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TCP连接中的一个信道，真正与服务器的通信端，调用核心API
 */
public class Channel {
    //Id标识
    private String channelId;
    //属于哪一个connection连接下
    private Connection connection;
    //存储服务器的响应，方便返回给客户端
    //key为请求-响应绑定标识rid，value为公共返回参数
    private ConcurrentHashMap<String, BasicReturns> returnsMap=new ConcurrentHashMap<>();
    //回调函数
    private Consumer consumer=null;
    public Channel(String channelId,Connection connection){
        this.channelId=channelId;
        this.connection=connection;
    }

    //1.告知服务器创建channel(本质就是让服务器那里“存储id”，交互时，可以找到信道),真正创建在connection就完成
    public boolean createChannel() throws IOException {
        //构造请求
        BasicArguments basicArguments=new BasicAckArguments();
        basicArguments.setChannelId(channelId);
        basicArguments.setRid(genereteRid());
        byte[] payload = BinaryTool.toByte(basicArguments);
        Request request=new Request();
        request.setType(0x1);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送请求
        connection.writeRequest(request);
        //等待响应
        BasicReturns basicReturns=waitReturn(basicArguments.getRid());
        return basicReturns.isOk();
    }

    //生成本次通信的rid绑定请求-响应
    private String genereteRid() {
        return "R-"+ UUID.randomUUID().toString();
    }

    //等待服务器响应
    private BasicReturns waitReturn(String rid) throws IOException {
        BasicReturns basicReturns=new BasicReturns();
        //阻塞等待响应
        synchronized (this){
            //查表，看是否有响应返回
            while ((basicReturns=returnsMap.get(rid))==null){
                try {
                    //无，则一直阻塞等待
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        //有则获取，并且删除表中返回，不占用空间
        returnsMap.remove(rid);
        return basicReturns;
    }

    //2.关闭channel，告诉服务器
    public boolean closeChannel() throws IOException {
        //构造请求
        BasicArguments basicArguments = new BasicAckArguments();
        basicArguments.setRid(genereteRid());
        basicArguments.setChannelId(channelId);
        byte[] payload = BinaryTool.toByte(basicArguments);
        Request request=new Request();
        request.setType(0x2);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送给服务器
        connection.writeRequest(request);
        //等待响应
        BasicReturns basicReturns=waitReturn(basicArguments.getRid());
        return basicReturns.isOk();
    }

    //3.channel调用服务器API，创建交换机
    public boolean exchangeDeclare(String exchangeName, exchangeType exchangeType, boolean durable, boolean autoDelete, Map<String,Object> arguments) throws IOException {
        //构造请求
        ExchangeDeclareArguments argument=new ExchangeDeclareArguments();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setExchangeName(exchangeName);
        argument.setExchangeType(exchangeType);
        argument.setDurable(durable);
        argument.setAutoDelete(autoDelete);
        argument.setArguments(arguments);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x3);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送请求
        connection.writeRequest(request);
        //等待响应
        BasicReturns basicReturns=waitReturn(argument.getRid());
        return basicReturns.isOk();
    }

    //4.销毁交换机
    public boolean exchangeDelete(String exchangeName) throws IOException {
        ExchangeDeleteArguments argument=new ExchangeDeleteArguments();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setExchangeName(exchangeName);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x4);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送请求
        connection.writeRequest(request);
        //等待响应
        BasicReturns basicReturns=waitReturn(argument.getRid());
        return basicReturns.isOk();
    }

    //5.创建队列
    public boolean queueDeclare(String queueName,boolean exclusive,boolean durable,boolean autoDelete,Map<String,Object> arguments) throws IOException {
        QueueDeclareArguments argument=new QueueDeclareArguments();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setQueueName(queueName);
        argument.setExclusive(exclusive);
        argument.setDurable(durable);
        argument.setAutoDelete(autoDelete);
        argument.setArguments(arguments);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x5);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns=waitReturn(argument.getRid());
        return basicReturns.isOk();
    }

    //6.销毁队列
    public boolean queueDelete(String queueName) throws IOException {
        QueueDeleteArguments argument=new QueueDeleteArguments();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setQueueName(queueName);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x6);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns=waitReturn(argument.getRid());
        return basicReturns.isOk();
    }

    //7.创建绑定
    public boolean bindingDeclare(String exchangeName,String queueName,String bindingKey) throws IOException {
        BindingDeclareArguments arguments=new BindingDeclareArguments();
        arguments.setRid(genereteRid());
        arguments.setChannelId(channelId);
        arguments.setExchangeName(exchangeName);
        arguments.setQueueName(queueName);
        arguments.setBindingKey(bindingKey);
        byte[] payload = BinaryTool.toByte(arguments);
        Request request=new Request();
        request.setType(0x7);
        request.setLength(payload.length);
        request.setPayload(payload);
        connection.writeRequest(request);
        BasicReturns basicReturns=waitReturn(arguments.getRid());
        return basicReturns.isOk();
    }

    //8.channel调用服务器API，删除绑定
    public boolean bindingDelete(Binding binding) throws IOException {
        //构造请求
        BindingDeleteArguments argument=new BindingDeleteArguments();
        argument.setRid(genereteRid());
        argument.setChannelId(channelId);
        argument.setBinding(binding);
        byte[] payload = BinaryTool.toByte(argument);
        Request request=new Request();
        request.setType(0x8);
        request.setLength(payload.length);
        request.setPayload(payload);
        //发送并且等待响应
        connection.writeRequest(request);
        BasicReturns basicReturns =waitReturn(argument.getRid());
        return basicReturns.isOk();
    }

    //9. 发送消息
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) throws IOException {
        BasicPublishArguments arguments = new BasicPublishArguments();
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
        BasicReturns basicReturns = waitReturn(arguments.getRid());
        return basicReturns.isOk();
    }


    // 10.订阅消息
    public boolean basicSubscribe(MessageQueue queue, boolean autoAck, Consumer consumer) throws IOException, mqException {
        // 先设置回调.
        if (this.consumer != null) {
            throw new mqException("该 channel 已经设置过消费消息的回调了, 不能重复设置!");
        }
        this.consumer = consumer;

        BasicSubscribeArguments arguments=new BasicSubscribeArguments();
        arguments.setRid(genereteRid());
        arguments.setChannelId(channelId);
        arguments.setConsumerTag(channelId);  // 此处 consumerTag 也使用 channelId 来表示了.
        arguments.setQueue(queue);
        arguments.setAutoAck(autoAck);
        byte[] payload = BinaryTool.toByte(arguments);

        Request request = new Request();
        request.setType(0xa);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitReturn(arguments.getRid());
        return basicReturns.isOk();
    }

    // 确认消息
    public boolean basicAck(MessageQueue queue, Message message) throws IOException {
        BasicAckArguments arguments = new BasicAckArguments();
        arguments.setRid(genereteRid());
        arguments.setChannelId(channelId);
        arguments.setQueue(queue);
        arguments.setMessage(message);
        byte[] payload = BinaryTool.toByte(arguments);
        Request request = new Request();
        request.setType(0xb);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BasicReturns basicReturns = waitReturn(arguments.getRid());
        return basicReturns.isOk();
    }

    //connection接收到了响应--->发给channel种存储哈希表
    public void putReturns(BasicReturns basicReturns) {
        returnsMap.put(basicReturns.getRid(),basicReturns);
        synchronized (this){
            //当前不知道多少线程等待这个响应
            //先把所有线程唤醒，对应这个响应的就会得到这个响应处理，其他的线程继续等待自己的响应
            notifyAll();
        }
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
}
