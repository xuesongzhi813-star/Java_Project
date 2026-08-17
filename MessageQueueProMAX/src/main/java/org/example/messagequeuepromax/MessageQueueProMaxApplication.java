package org.example.messagequeuepromax;

import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.BrokerServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;

@SpringBootApplication
public class MessageQueueProMaxApplication {

    public static ConfigurableApplicationContext context;
    public static void main(String[] args) throws IOException, mqException {

        context=SpringApplication.run(MessageQueueProMaxApplication.class, args);
        BrokerServer brokerServer=new BrokerServer(9090);
        brokerServer.start();
    }

}
