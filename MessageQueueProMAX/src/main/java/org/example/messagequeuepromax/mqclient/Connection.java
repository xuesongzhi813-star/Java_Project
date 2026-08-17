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
    public Connection(String host,int port) throws IOException {
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
                throw new mqException("[Connection] 消息对应的channel并不存在:"+channel.getChannelId());
            }
            callback.submit(()->{
                try {
                    channel.getConsumer().deliverMessage(subScribeReturns.getConsumerTag()
                            ,subScribeReturns.getBasicProperties(), subScribeReturns.getBody());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }else {
            //当前响应需要处理
            //先解析
            BasicReturns basicReturns= (BasicReturns) BinaryTool.toObject(response.getPayload());
            //根据channelId，找到该响应对应的channel
            Channel channel=channelMap.get(basicReturns.getChannelId());
            if(channel==null){
                throw new mqException("[Connection] 该响应对应的channel并不存在:"+channel.getChannelId());
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

    //3.创建channel
    public Channel createChannel(){
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
        //
        if(!ok){
            //创建失败则进行回滚操作
            channelMap.remove(channel.getChannelId());
        }
        System.out.println("[Connection] 创建channel信道成功:"+channel.getChannelId());
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
