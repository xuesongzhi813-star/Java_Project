package org.example.messagequeuepromax;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class MessageQueueProMaxApplication {

    public static ConfigurableApplicationContext context;
    public static void main(String[] args) {

        context=SpringApplication.run(MessageQueueProMaxApplication.class, args);
    }

}
