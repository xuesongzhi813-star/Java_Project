package org.example.mymessagequeue.mqclient;

import org.example.mymessagequeue.common.*;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Connection {
    //socket对象进行通信连接
    private Socket socket=null;
    //管理一个TCP连接（当前连接）下的建立起来的channel对话
    //key为channelId，value为对应
    private ConcurrentHashMap<String,Channel> map=new ConcurrentHashMap<>();
    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;
    ExecutorService callback= null;

    public Connection(String host, int port) throws IOException {
        socket=new Socket(host,port);
        inputStream=socket.getInputStream();
        outputStream=socket.getOutputStream();
        dataInputStream=new DataInputStream(inputStream);
        dataOutputStream=new DataOutputStream(outputStream);
        callback=Executors.newFixedThreadPool(4);
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
            catch (IOException | mqException|ClassNotFoundException e) {
                System.out.println("[Connection] 扫描线程读取响应失败，socket通信异常");
                e.printStackTrace();
            }
        });
        t.start();
    }

    public void close() {
        callback.shutdownNow();
        map.clear();
        try {
            inputStream.close();
            outputStream.close();
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    //处理扫描线程读取到的响应
    private void dispatchResponse(Response response) throws IOException, ClassNotFoundException, mqException {
        //若是推送消息
        if(response.getType()==0xc){
            //先解析payload，其实就是SubScribeReturns
            SubScribeReturns subScribeReturns= (SubScribeReturns) BinaryTool.toObject(response.getPayload());
            //根据channelId，调用channel的回调函数处理该消息
            Channel channel = map.get(subScribeReturns.getChannelId());
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
            Channel channel=map.get(basicReturns.getChannelId());
            if(channel==null){
               throw new mqException("[Connection] 该响应对应的channel并不存在:"+channel.getChannelId());
            }
            channel.putReturns(basicReturns);
        }
    }


    //发送请求
    public void writeRequest(Request request) throws IOException {
        dataOutputStream.writeInt(request.getType());
        dataOutputStream.writeInt(request.getLength());
        dataOutputStream.write(request.getPayload());
        dataOutputStream.flush();
    }

    //接收响应
    public Response readResponse() throws IOException {
        Response response=new Response();
        response.setType(dataInputStream.readInt());
        response.setLength(dataInputStream.readInt());
        byte[] payload=new byte[response.getLength()];
        int read = dataInputStream.read(payload);
        if(read!=response.getLength()){
            throw new IOException("[Connection] 读取到的响应数据有缺失");
        }
        response.setPayload(payload);
        System.out.println("[Connection] 收到响应 type:"+response.getType()+",length:"+response.getLength());
        return response;
    }

    //创建channel
    public Channel createChannel() throws IOException {
        Channel channel = new Channel("C-" + UUID.randomUUID().toString(), this);
        //放入socket对应的channel哈希表中
        map.put(channel.getChannelId(),channel);
        //告知服务器创建channel（调用API）
        boolean ok = channel.ChcreateChannel();
        //创建失败则进行“回滚”操作，把表中插入的“新创建channel”取出
        if (ok == false) {
            map.remove(channel.getChannelId());
            return null;
        }
        return channel;
    }
}
