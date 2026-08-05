package org.example.mymessagequeue;

import org.example.mymessagequeue.mqserver.mapper.MetaMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class MyMessageQueueApplication {

    //通过设置全局静态context注入MetaMapper

   public static ConfigurableApplicationContext context;
    public static void main(String[] args) {
        context = SpringApplication.run(MyMessageQueueApplication.class, args);
    }

}
