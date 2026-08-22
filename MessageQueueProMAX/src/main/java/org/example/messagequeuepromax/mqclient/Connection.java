package org.example.messagequeuepromax.mqclient;

import org.example.messagequeuepromax.common.*;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 一个TCP连接，持有socket通信，管理多个channel进行交互
 */
public class Connection {
    //Socket对象进行通信
    private Socket socket;
    //管理当前TCP连接下的channel
    private ConcurrentHashMap<String,Channel> channelMap=new ConcurrentHashMap<>();
    //线程池处理，多个响应情况
    private ExecutorService callback;
    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    //用户的信息
    private String userName;
    private String password;

    public Connection(String host,int port,String userName,String password) throws IOException {
        this.userName=userName;
        this.password=password;
        socket=new Socket(host,port);
        inputStream=socket.getInputStream();
        outputStream=socket.getOutputStream();
        dataInputStream=new DataInputStream(inputStream);
        dataOutputStream=new DataOutputStream(outputStream);
        callback= Executors.newFixedThreadPool(4);
        //创建一个扫描线程，从socket中读取响应数据，并且交给channel处理
        Thread t=new Thread(()->{
            try {
                while (!socket.isClosed()) {
                    Response response = readResponse();
                    dispatchResponse(response);
                }
            }catch (SocketException e) {
                System.out.println("[Connection] 当前socket通信已经关闭");
            }
            catch (IOException | mqException | ClassNotFoundException e) {
                System.out.println("[Connection] 扫描线程读取响应失败，socket通信异常");
                e.printStackTrace();
            }
        });
        t.start();
    }

    //处理扫描线程读取到的响应
    private void dispatchResponse(Response response) throws IOException, ClassNotFoundException, mqException {
        //若是推送消息
        if(response.getType()==0xc){
            //先解析payload，其实就是SubScribeReturns
            SubscribeReturns subScribeReturns= (SubscribeReturns) BinaryTool.toObject(response.getPayload());
            //根据channelId，调用channel的回调函数处理该消息
            Channel channel = channelMap.get(subScribeReturns.getChannelId());
            if(channel==null){
                //注：channel为null时不能调channel.getChannelId()（会空指针），日志要从报文里取channelId
                throw new mqException("[Connection] 消息对应的channel并不存在:"+subScribeReturns.getChannelId());
            }
            callback.submit(()->{
                try {
                    channel.getConsumer().deliverMessage(subScribeReturns.getConsumerTag()
                            ,subScribeReturns.getBasicProperties(), subScribeReturns.getBody());
                } catch (IOException | mqException e) {
                    throw new RuntimeException(e);
                }
            });
        }else {
            //当前响应需要处理
            //先解析
            BasicReturns basicReturns= (BasicReturns) BinaryTool.toObject(response.getPayload());
            //0xd=未登录拒绝响应：payload同样是BasicReturns(ok=false)，照常按rid配对唤醒等待线程
            //但必须与普通业务失败区分开：明确打日志提示是认证问题，而不是让调用方误以为是业务返回false
            if(response.getType()==0xd){
                System.out.println("[Connection] 业务请求被服务器拒绝(channel未登录或登录已失效):channelId:"
                        +basicReturns.getChannelId()+",rid:"+basicReturns.getRid());
            }
            //根据channelId，找到该响应对应的channel
            Channel channel=channelMap.get(basicReturns.getChannelId());
            if(channel==null){
                //注：channel为null时不能调channel.getChannelId()（会空指针），日志要从报文里取channelId
                throw new mqException("[Connection] 该响应对应的channel并不存在:"+basicReturns.getChannelId());
            }
            channel.putReturns(basicReturns);
        }
    }

    //1.发送请求
    public void writeRequest(Request request) throws IOException {
        dataOutputStream.writeInt(request.getType());
        dataOutputStream.writeInt(request.getLength());
        dataOutputStream.write(request.getPayload());
    }

    //2.读取响应
    public Response readResponse() throws IOException {
        Response response=new Response();
        response.setType(dataInputStream.readInt());
        response.setLength(dataInputStream.readInt());
        byte[] payload=new byte[response.getLength()];
        int n=dataInputStream.read(payload);
        if(n!= response.getLength()){
            throw new IOException("[Connection] 响应长度读取异常");
        }
        response.setPayload(payload);
        return response;
    }

    //3.创建channel（不登录版本）：仅告知服务器创建channel，不做认证
    //用途：“先注册后登录”场景——数据库没有该用户信息时，必须先拿到channel才能发注册请求，
    //     而带参版本登录失败会回滚不返回channel，形成“无法注册”的死循环，故提供此解耦入口
    //安全：未认证channel的业务请求会被服务器拦截门(type=0xd)拒绝，除login/register/closeChannel外什么都做不了
    public Channel createChannel() throws IOException, mqException {
        Channel channel=new Channel("Ch-"+ UUID.randomUUID().toString(),this);
        //放入表中
        channelMap.put(channel.getChannelId(),channel);
        //告知服务器创建channel
        boolean ok= false;
        try {
            ok = channel.createChannel();
        } catch (IOException e) {
            System.out.println("[Connection] 创建channel失败:"+channel.getChannelId());
        }
        if(!ok){
            //创建失败则进行回滚操作，不能把不可用的channel交出去
            channelMap.remove(channel.getChannelId());
            throw new mqException("[Connection] channel创建失败:"+channel.getChannelId());
        }
        System.out.println("[Connection] 创建channel信道成功(未认证):"+channel.getChannelId());
        return channel;
    }

    //创建channel并立即用给定凭证登录（便捷版本）：登录失败回滚并抛认证异常
    public Channel createChannel(String userName,String password) throws IOException, mqException {
        //复用不登录版本完成channel创建
        Channel channel=createChannel();
        //建立连接后，马上进行登录请求
        //登录失败：本channel未通过认证，服务器会拦截后续所有业务请求，必须直接失败
        boolean loginOk=false;
        try {
            loginOk=channel.login(userName,password);
        } catch (IOException e) {
            System.out.println("[Connection] 登录请求发送失败:"+channel.getChannelId());
        }
        if(!loginOk){
            //回滚并抛出认证异常，避免调用方拿着未认证的channel继续操作、被服务器静默拦截成莫名其妙的false
            channelMap.remove(channel.getChannelId());
            throw new mqException("[Connection] 认证失败:用户名或密码错误,userName:"+userName
                    +",channelId:"+channel.getChannelId());
        }
        return channel;
    }

    public void close() {
        callback.shutdownNow();
        channelMap.clear();
        try {
            inputStream.close();
            outputStream.close();
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
