package org.example.mymessagequeue;

import org.example.mymessagequeue.mqserver.BrokerServer;
import org.example.mymessagequeue.mqserver.mapper.MetaMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;

@SpringBootApplication
public class MyMessageQueueApplication {

    //通过设置全局静态context注入MetaMapper

   public static ConfigurableApplicationContext context;
    public static void main(String[] args) throws IOException {
        context = SpringApplication.run(MyMessageQueueApplication.class, args);
        BrokerServer brokerServer=new BrokerServer(9090);
        brokerServer.start();
    }

}
