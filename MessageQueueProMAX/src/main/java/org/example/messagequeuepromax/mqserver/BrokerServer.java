package org.example.messagequeuepromax.mqserver;

import org.example.messagequeuepromax.common.*;
import org.example.messagequeuepromax.mqserver.core.BasicProperties;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 服务器的实现代码：
 */
public class BrokerServer {
    //“服务器”接收“客户端”请求的“前台接待”-->最终每个被接待的“客户端”，都会有一个独属的“clientSocket”
    private ServerSocket serverSocket;
    //考虑此时只有一个“虚拟主机”的情况
    private VirtualHost virtualHost=new VirtualHost("default");
    //哈希表，key表示channelId表示当前通信是哪个通道，value表示clientSocket表示当前通道，通信的载体
    private ConcurrentHashMap<String, Socket> connectionMap=new ConcurrentHashMap<>();
    //线程池，处理多个客户端的请求
    private ExecutorService executorService;
    //控制服务器运行的开关-->volatile是为了让服务器能感知到“取值变化”，及时做出应答
    private volatile boolean ok=true;

    public BrokerServer(int port) throws IOException {
        this.serverSocket=new ServerSocket(port);
    }

    //启动服务器
    public void start() throws mqException, IOException {
        System.out.println("[BrokerServer] 服务器启动成功");
        //线程池创建
        executorService= Executors.newCachedThreadPool();
        //开始一直运行服务器
        while (ok){
            //接收客户端请求，创建通信clientSocket
            try {
                Socket clientSocket = serverSocket.accept();
                //提交线程池，处理socket客户端的请求
                executorService.submit(()->{
                   proccessConnection(clientSocket);
                });
            } catch (IOException e) {
                System.out.println("[BrokerServer] 服务器结束运行");
            }
        }
    }


    //关闭服务器
    public void close() throws IOException {
        //循环结束
        ok=false;
        //关闭线程池
        executorService.shutdownNow();

        //关闭前台接待
        serverSocket.close();
    }

    //处理客户端请求
    private void proccessConnection(Socket clientSocket) {
        //定义的请求/响应格式中有二进制数据的存在，需要使用DataInputStream+DataOutputStream
        try (InputStream inputStream=clientSocket.getInputStream();
             OutputStream outputStream=clientSocket.getOutputStream()){
            try (DataInputStream dataInputStream=new DataInputStream(inputStream);
                 DataOutputStream dataOutputStream=new DataOutputStream(outputStream)) {
                while (true) {
                    //1.读取请求信息
                    Request request = readRequest(dataInputStream);
                    //2.根据请求，处理业务+产出响应
                    Response response = proccess(request, clientSocket);
                    //3.将响应发送回客户端
                    writeResponse(dataOutputStream, response);
                }
            }
        } catch (IOException | mqException e) {
            System.out.println("[BrokerServer] 服务器-客户端连接异常");
        }finally {
            //关闭socket+当前channel关闭
            try {
                clientSocket.close();
                //关闭channel
                closeChannel(clientSocket);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void closeChannel(Socket clientSocket) {
        List<String> list=new ArrayList<>();
        //只收集"当前关闭的这个socket"对应的channel，不能把其他连接（如消费者的channel）一并清掉
        for (Map.Entry<String,Socket> entry:connectionMap.entrySet()){
            if(entry.getValue()==clientSocket){
                list.add(entry.getKey());
            }
        }
        //删除当前socket对应的channel（因为socket已关闭）
        for (String channelId:list){
            connectionMap.remove(channelId);
            //消费者连接断开时，同步清理该 channel（consumerTag）对应的订阅，避免死订阅继续占位
            virtualHost.getMemoryDataCenter().removeConsumerByTag(channelId);
        }
        System.out.println("[BrokerServer] channel信道关闭完成:"+clientSocket.getInetAddress().toString());
    }

    //读取请求信息，即读取三次，填充好“自定义request格式的内容”
    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        Request request=new Request();
        request.setType(dataInputStream.readInt());
        request.setLength(dataInputStream.readInt());
        byte[] payload=new byte[request.getLength()];
        int n=dataInputStream.read(payload);
        if(n!= request.getLength()){
            throw new IOException("[BrokerServer] 读取请求长度异常");
        }
        request.setPayload(payload);
        return request;
    }

    //响应发送回客户端
    private void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        //先写类型
        dataOutputStream.writeInt(response.getType());
        //再写长度
        dataOutputStream.writeInt(response.getLength());
        //最后写主体
        dataOutputStream.write(response.getPayload());
    }

    //处理请求
    private Response proccess(Request request, Socket clientSocket) throws mqException {
        //先解析request的所有部分
        //type是调用API是啥，length暂时不管，payload要反序列化成BasicArguments
        BasicArguments basicArguments= (BasicArguments) BinaryTool.toObject(request.getPayload());
        //打印request属性
        System.out.println("[BrokerServer] request解析成功:type:"+request.getType()+",length:"+request.getLength()+
                ",rid:"+basicArguments.getRid()+",channelId:"+basicArguments.getChannelId());
        //根据type值解析请求目的
        //本次请求的处理结果
        boolean result=true;
        if(request.getType()==0x1){
            //创建channel
            connectionMap.put(basicArguments.getChannelId(),clientSocket);
            System.out.println("[BrokerServer] 创建channel成功，channelId:"+basicArguments.getChannelId());
        } else if (request.getType()==0x2) {
            //关闭channel
            connectionMap.remove(basicArguments.getChannelId());
            //关闭channel时，同步清理该消费者（consumerTag==channelId）的订阅，避免死订阅继续占位
            virtualHost.getMemoryDataCenter().removeConsumerByTag(basicArguments.getChannelId());
            System.out.println("[BrokerServer] 关闭channel成功，channelId:"+basicArguments.getChannelId());
        } else if (request.getType()==0x3) {
            //创建交换机
            ExchangeDeclareArguments arguments= (ExchangeDeclareArguments) basicArguments;
            result=virtualHost.exchangeDeclare(arguments.getExchangeName(), arguments.getExchangeType(),arguments.isDurable(),
            arguments.isAutoDelete(),arguments.getArguments());
        } else if (request.getType()==0x4) {
            //销毁交换机
            ExchangeDeleteArguments arguments= (ExchangeDeleteArguments) basicArguments;
            result=virtualHost.exchangeDelete(arguments.getExchangeName());
        } else if (request.getType()==0x5) {
            //创建队列
            QueueDeclareArguments arguments= (QueueDeclareArguments) basicArguments;
            result= virtualHost.queueDeclare(arguments.getQueueName(),arguments.isExclusive(),arguments.isDurable()
            ,arguments.isAutoDelete(),arguments.getArguments());
        } else if (request.getType()==0x6) {
            //销毁队列
            QueueDeleteArguments arguments= (QueueDeleteArguments) basicArguments;
            result=virtualHost.queueDelete(arguments.getQueueName());
        } else if (request.getType()==0x7) {
            //创建绑定
            BindingDeclareArguments arguments= (BindingDeclareArguments) basicArguments;
            result=virtualHost.bindingDeclare(arguments.getExchangeName(), arguments.getQueueName(), arguments.getBindingKey());
        } else if (request.getType()==0x8) {
            //删除绑定
            BindingDeleteArguments arguments= (BindingDeleteArguments) basicArguments;
            result=virtualHost.bindingDelete(arguments.getBinding());
        } else if (request.getType()==0x9) {
            //发布消息
            BasicPublishArguments arguments= (BasicPublishArguments) basicArguments;
            result=virtualHost.basicPublish(arguments.getExchangeName(), arguments.getRoutingKey(), arguments.getBasicProperties(), arguments.getBody());
        } else if (request.getType()==0xa) {
            //订阅消息
            BasicSubscribeArguments arguments= (BasicSubscribeArguments) basicArguments;
            result=virtualHost.basicSubscribe(arguments.getQueue(), arguments.getConsumerTag(), arguments.isAutoAck()
                    , new Consumer() {
                        @Override
                        public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) throws IOException {
                            //消息推送回对应客户端
                            //根据channel（consumerTag）获得客户端的socket
                            Socket socket = connectionMap.get(consumerTag);
                            if(socket==null || socket.isClosed()){
                                System.out.println("[BrokerServer] 推送消息的目标客户端已关闭，无法推送:consumerTag:"+consumerTag);
                                //必须抛异常，让 ConsumerManager 感知到"投递失败"，把消息重新入队而不是静默丢弃
                                throw new IOException("[BrokerServer] 消费者连接已关闭，消息推送失败:consumerTag:"+consumerTag);
                            }
                            //构造响应数据
                            SubscribeReturns subscribeReturns=new SubscribeReturns();
                            subscribeReturns.setConsumerTag(consumerTag);
                            subscribeReturns.setBasicProperties(basicProperties);
                            subscribeReturns.setBody(body);
                            subscribeReturns.setRid("");
                            //当前设计里 consumerTag 就代表 channelId，必须设置，否则客户端 channelMap.get(null) 会 NPE
                            subscribeReturns.setChannelId(consumerTag);
                            byte[] payload = BinaryTool.toByte(subscribeReturns);
                            //构造响应
                            Response response=new Response();
                            //推送消息给消费者
                            response.setType(0xc);
                            response.setLength(payload.length);
                            response.setPayload(payload);
                            //响应写入客户端
                            DataOutputStream dataOutputStream=new DataOutputStream(socket.getOutputStream());
                            writeResponse(dataOutputStream,response);
                        }
                    });
        } else if (request.getType()==0xb) {
            //手动应答
            BasicAckArguments arguments= (BasicAckArguments) basicArguments;
            result=virtualHost.basicAck(arguments.getQueue(), arguments.getMessage());
        }else {
            //非法type
            throw new mqException("[BrokerServer] 请求typeAPI异常，请检查type:"+request.getType());
        }
        //构造BasicReturns
        BasicReturns basicReturns=new BasicReturns();
        basicReturns.setRid(basicArguments.getRid());
        basicReturns.setChannelId(basicArguments.getChannelId());
        basicReturns.setOk(result);
        byte[] payload = BinaryTool.toByte(basicReturns);
        //构造响应，返回
        Response response=new Response();
        response.setType(response.getType());
        response.setLength(payload.length);
        response.setPayload(payload);
        return response;
    }

}
