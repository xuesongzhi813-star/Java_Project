package org.example.messagequeuepromax.demo;

import org.example.messagequeuepromax.common.Consumer;
import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqclient.Channel;
import org.example.messagequeuepromax.mqclient.Connection;
import org.example.messagequeuepromax.mqclient.ConnectionFactory;
import org.example.messagequeuepromax.mqserver.core.BasicProperties;
import org.example.messagequeuepromax.mqserver.core.DeadLetterInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConsumerDemo {
    public static void main(String[] args) throws IOException, mqException, InterruptedException {
        System.out.println("启动消费者");

        ConnectionFactory factory=new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(9090);

        Connection connection=factory.createConnection();

        //一个channel只能挂一个消费者回调（basicSubscribe重复设置会抛异常，之前报错的根源），
        //所以业务队列和死信队列各用一个channel
        //业务channel：首次运行自动注册；再次运行(账户已存在)注册失败，改为直接登录
        Channel channel=connection.createChannel();
        if(!channel.register("consumerUser","123456")){
            channel.login("consumerUser","123456");
        }
        //死信channel：用户已注册过，直接登录
        Channel dlChannel=connection.createChannel();
        dlChannel.login("consumerUser","123456");

        //创建交换机和队列(存在就不会再创建,都创建一下不影响)
        channel.exchangeDeclare("testExchange", exchangeType.DIRECT,true,false,null);

        //配置死信交换机+死信队列（队列名 = 死信交换机名 + "_queue"，与服务器端 routeToDeadLetter 约定一致）
        channel.exchangeDeclare("dlxExchange",exchangeType.DIRECT,true,false,null);
        //死信队列本身只起接收存储作用，不用配置死信信息
        channel.queueDeclare("dlxExchange_queue",false,true,false,null);

        //配置死信信息进入queue的额外选项arguments
        //键名与服务器端统一：x-death-exchange=死信交换机名，x-max-retry=最大重投次数(超过转死信)
        Map<String,Object> arguments=new HashMap<>();
        arguments.put("x-death-exchange","dlxExchange");
        arguments.put("x-max-retry",2);
        //业务队列带死信配置声明，死信时才知道要发送去哪
        //注意：队列已存在时重复声明不会更新arguments！若 testQueue 之前用 null 声明过，
        //需先 channel.queueDelete("testQueue") 或删除服务器 ./data 目录后重启
        channel.queueDeclare("testQueue",true,false,true,arguments);

        //先订阅死信队列（autoAck=true：收到即确认）
        dlChannel.basicSubscribe("dlxExchange_queue", true, new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] body) throws IOException {
                System.out.println("========== [死信队列] 收到死信 ==========");
                System.out.println("[死信] body="+new String(body,0,body.length));
                if(basicProperties.getHeaders()!=null){
                    DeadLetterInfo info=(DeadLetterInfo) basicProperties.getHeaders().get("x-death");
                    if(info!=null){
                        System.out.println("[死信] 死因:"+info.getReason());
                        System.out.println("[死信] 原队列:"+info.getOriginalQueue());
                        System.out.println("[死信] 原routingKey:"+info.getOriginalRoutingKey());
                        System.out.println("[死信] 成为死信时间戳:"+info.getDeadLetterAt());
                        System.out.println("[死信] 第"+info.getCount()+"次成为死信");
                    }
                }
                System.out.println("========================================");
            }
        });

        //=====再订阅业务队列（autoAck=false：手动应答模式，才能在回调里拒绝触发死信）=====
        channel.basicSubscribe("testQueue", false, new Consumer() {
            @Override
            public void deliverMessage(String consumerTag, BasicProperties basicProperties, byte[] bytes) throws IOException {
                System.out.println("[业务] 收到消息:"+new String(bytes,0,bytes.length)
                        +",messageId:"+basicProperties.getMessageId());

                //=====测试路径一：REJECTED（requeue=false，直接拒绝不重投，立刻转死信）=====
//                boolean ok=channel.basicReject("testQueue",basicProperties.getMessageId(),false);

                //=====测试路径二：MAX_RETRY（requeue=true，拒绝但要求重投，投满 x-max-retry 次后转死信）=====
                //测这条路径：注释掉上面一行，放开下面这行。预期收到同一条消息3次后转死信
                boolean ok=channel.basicReject("testQueue",basicProperties.getMessageId(),true);

                System.out.println("[业务] 已拒绝(该消息将走死信流程):ok:"+ok);
            }
        });

        //模拟一直"等待消息"，"消费消息"
        while (true){
            Thread.sleep(500);
        }
    }
}
