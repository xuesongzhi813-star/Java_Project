package org.example.mymessagequeue.mqserver;

import org.example.mymessagequeue.common.*;
import org.example.mymessagequeue.mqserver.coreentity.BasicProperties;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * 服务器的实现代码:
 */
public class BrokerServer {
    //相当于前台，负责接待“客户端的socket”给独属于的socket通道
    private ServerSocket serverSocket=null;
    //考虑只有一个虚拟主机
    private VirtualHost virtualHost=new VirtualHost("default");
    //线程池，处理多客户端的请求
    private ExecutorService executorService=null;
    //哈希表，key:channelId（表示是哪个客户端）,value:服务器socket表示哪个负责处理这个客户端请求信息
    private ConcurrentHashMap<String, Socket> map=new ConcurrentHashMap<>();
    //循环控制变量，表示服务器的运行状态开关
    private volatile boolean runnale=true;

    //绑定端口号
    public BrokerServer(int port) throws IOException {
        this.serverSocket=new ServerSocket(port);
    }

    /**
     * 服务器系列方法：
     * 1.启动服务器
     * 2.关闭服务器
     * 3.处理客户端-服务器连接
     */
    //启动服务器
    public void start() throws IOException {
        System.out.println("[BrokerServer] 服务器启动成功！");
        //创建线程池，处理多客户端
        executorService= Executors.newCachedThreadPool();
        try {
            while (runnale) {
                //接待客户端
                Socket socket = serverSocket.accept();
                //提交线程
                executorService.submit(() -> {
                        //处理socket
                    try {
                        proccessConnection(socket);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }catch (SocketException e){
            System.out.println("[BrokerServer] 服务器启动失败，异常");
        }
    }

    //关闭服务器
    public void close() throws IOException {
        runnale=false;
        //关闭线程池
        executorService.close();
        //关闭前台接待
        serverSocket.close();
    }

    private void proccessConnection(Socket socket) throws IOException {
        try (InputStream inputStream=socket.getInputStream();
             OutputStream outputStream=socket.getOutputStream()){
            //因为要处理“二进制数据”，使用Data相关流
            try (DataInputStream dataInputStream=new DataInputStream(inputStream);
                 DataOutputStream dataOutputStream=new DataOutputStream(outputStream)){
                //1.读取请求
                Request request=readRequest(dataInputStream);
                //2.根据请求，处理请求，得到响应信息
                Response response =proccess(request,socket);
                //3.返回响应
                writeResponse(response,dataOutputStream);
            }catch (EOFException eofException){
                //出现eof异常视为读取请求完毕
                System.out.println("[BrokerServer] 请求读取完毕，连接结束"+socket.getInetAddress().toString());
                eofException.printStackTrace();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            System.out.println("[BrokerServer] 连接出现异常"+socket.getInetAddress().toString());
        }finally {
            socket.close();
            //关闭当前会话的channel
            closeChannel(socket);
        }
    }

    private void closeChannel(Socket socket) {
    }

    private void writeResponse(Response response, DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(response.getType());
        dataOutputStream.writeInt(response.getLength());
        dataOutputStream.write(response.getPayload());
        dataOutputStream.flush();
        System.out.println("[BrokerServer] 响应返回客户端成功");
    }

    private Response proccess(Request request, Socket socket) throws IOException, ClassNotFoundException, mqException {
        //先解析request中的payload，参数获取(先获取为“公共参数”)
        BasicArguments basicArguments= (BasicArguments) BinaryTool.toObject(request.getPayload());
        System.out.println("[BrokerServer] 解析成功:rid:"+basicArguments.getRid()+",channelId:"+basicArguments.getChannelId()+
                ",type:"+request.getType()+",length:"+request.getLength());
        //根据type的解析值，决定调用哪个API方法
        //本次“请求”处理的结果
        boolean ok=true;
        if(request.getType()==0x1){

        } else if (request.getType()==0x2) {

        } else if (request.getType()==0x3) {
            //basicArguments就是参数
            ExchangeDeclareArgument exchangeDeclareArgument= (ExchangeDeclareArgument) basicArguments;
            //调用API传参
            ok=virtualHost.exchangeDeclare(exchangeDeclareArgument.getExchangeName(),exchangeDeclareArgument.getExchangetype(),
                    exchangeDeclareArgument.isDurable(),exchangeDeclareArgument.isAutoDelete(),exchangeDeclareArgument.getArguments());
        } else if (request.getType()==0x4) {
            ExchangeDeleteArgument argument= (ExchangeDeleteArgument) basicArguments;
            ok=virtualHost.exchangeDelete(argument.getExchangeName());
        } else if (request.getType()==0x5) {
            QueueDeclareArgument argument= (QueueDeclareArgument) basicArguments;
            ok=virtualHost.queueDeclare(argument.getQueueName(),argument.isDurable(),argument.isExclusive(),
                    argument.isAutoDelete(), argument.getArguments());
        } else if (request.getType()==0x6) {
            QueueDeleteArgument argument= (QueueDeleteArgument) basicArguments;
            ok=virtualHost.queueDelete(argument.getQueueName());
        } else if (request.getType()==0x7) {
            BindingDeclareArgument argument= (BindingDeclareArgument) basicArguments;
            ok=virtualHost.bindingDeclare(argument.getExchangeName(), argument.getQueueName(), argument.getBindingKey());
        } else if (request.getType()==0x8) {
            BindingDeleteArgument argument= (BindingDeleteArgument) basicArguments;
            ok=virtualHost.bindingDelete(argument.getQueueName(), argument.getExchangeName());
        }else if (request.getType()==0x9){
            BasicPublishArgument argument= (BasicPublishArgument) basicArguments;
            ok=virtualHost.basicPublish(argument.getExchangeName(), argument.getRoutingKey(),
                    argument.getBasicProperties(), argument.getBody());
        } else if (request.getType()==0xa) {
            BasicConsumeArgument argument= (BasicConsumeArgument) basicArguments;
            ok=virtualHost.basicConsume(argument.getConsumerTag(), argument.getQueueName(), argument.isAutoAck(),
                    new Consumer() {
                        @Override
                        public void deliverMessage(String conseumerTag, BasicProperties basicProperties, byte[] bytes) {
                            //通过channelId（cosumerTag）找socket对象
                            Socket clientSocket=map.get(conseumerTag);
                            if(clientSocket==null || clientSocket.isClosed())
                            {
                                System.out.println("[BrokerServer] 订阅消息的客户端已经关闭");
                            }
                            //构造响应数据

                        }
                    });
        }
        return null;
    }

    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        Request request=new Request();
        request.setType(dataInputStream.readInt());
        request.setLength(dataInputStream.readInt());
        byte[] bytes=new byte[request.getLength()];
        int n = dataInputStream.read(bytes);
        if(n!=request.getLength()){
            throw new IOException("[BrokerServer] 请求读取异常");
        }
        request.setPayload(bytes);
        return request;
    }

}
