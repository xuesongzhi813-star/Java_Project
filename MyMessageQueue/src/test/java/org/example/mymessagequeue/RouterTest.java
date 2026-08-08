package org.example.mymessagequeue;

import org.example.mymessagequeue.mqserver.coreentity.Binding;
import org.example.mymessagequeue.mqserver.coreentity.Message;
import org.example.mymessagequeue.mqserver.coreentity.Router;
import org.example.mymessagequeue.mqserver.coreentity.exchangetype;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class RouterTest {
    Router router=new Router();
    Binding binding=null;
    Message message=null;
    /**
     * 构造测试环境：
     * setUp：创建binding，message对象
     * tearDown：销毁binding，message对象
     */
    @BeforeEach
    public void setUp(){
         binding=new Binding();
         message=new Message();
    }

    @AfterEach
    public void tearDown(){
        binding=null;
        message=null;
    }

    /**
     * 对测试一系列用例的测试：
     */
    // [测试用例]
    // binding key          routing key         result
    // 1.aaa                  aaa                 true
    // 2.aaa.bbb              aaa.bbb             true
    // 3.aaa.bbb              aaa.bbb.ccc         false
    // 4.aaa.bbb              aaa.ccc             false
    // 5.aaa.bbb.ccc          aaa.bbb.ccc         true
    // 6.aaa.*                aaa.bbb             true
    // 7.aaa.*.bbb            aaa.bbb.ccc         false
    // 8. *.aaa.bbb            aaa.bbb             false
    // 9. #                    aaa.bbb.ccc         true
    // 10.aaa.#                aaa.bbb             true
    // 11.aaa.#                aaa.bbb.ccc         true
    // 12.aaa.#.ccc            aaa.ccc             true
    // 13.aaa.#.ccc            aaa.bbb.ccc         true
    // 14.aaa.#.ccc            aaa.aaa.bbb.ccc     true
    // 15. #.ccc                ccc                 true
    // 16. #.ccc                aaa.bbb.ccc         true
    @Test
    public void test1(){
        binding.setBindingKey("aaa");
        message.setRoutingKey("aaa");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }

    @Test
    public void test2(){
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test3(){
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertFalse(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test4(){
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.ccc");
        Assertions.assertFalse(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test5(){
        binding.setBindingKey("aaa.bbb.ccc");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test6(){
        binding.setBindingKey("aaa.*");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test7(){
        binding.setBindingKey("aaa.*.bbb");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertFalse(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test8(){
        binding.setBindingKey("*.aaa.bbb");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertFalse(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test9(){
        binding.setBindingKey("#");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test10(){
        binding.setBindingKey("aaa.#");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test11(){
        binding.setBindingKey("aaa.#");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test12(){
        binding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.ccc");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test13(){
        binding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test14(){
        binding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.aaa.bbb.ccc");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test15(){
        binding.setBindingKey("#.ccc");
        message.setRoutingKey("ccc");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
    @Test
    public void test16(){
        binding.setBindingKey("#.ccc");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(Router.routeTopic(exchangetype.TOPIC,binding,message));
    }
}
