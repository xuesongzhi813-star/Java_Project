package org.example.messagequeuepromax;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.example.messagequeuepromax.mqserver.VirtualHost;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 用户登录与认证（服务端逻辑层）验证：
 * 1.正确凭证登录成功（guest为init种子的默认账户）
 * 2.错误密码/不存在用户/null凭证 -> 统一失败（不NPE、不泄露用户是否存在）
 * 3.注册成功后可用注册凭证登录；重复注册被拒
 * 4.服务器"重启"（重建VirtualHost）后用户数据仍在（recovery加载用户表）
 */
@SpringBootTest
public class LoginTest {

    private VirtualHost virtualHost;

    @BeforeEach
    public void setUp(){
        MessageQueueProMaxApplication.context= SpringApplication.run(MessageQueueProMaxApplication.class);
        virtualHost=new VirtualHost("default");
    }

    @AfterEach
    public void tearDown(){
        MessageQueueProMaxApplication.context.close();
        virtualHost.getDiskDataCenter().getDataBaseManager().deleteAll();
        virtualHost=null;
    }

    //guest默认账户：正确凭证登录成功
    @Test
    public void loginSuccessWithGuest(){
        Assertions.assertTrue(virtualHost.login("guest","guest"),"默认guest账户应能登录");
    }

    //错误密码：失败
    @Test
    public void loginFailWithWrongPassword(){
        Assertions.assertFalse(virtualHost.login("guest","wrong-password"),"错误密码应登录失败");
    }

    //不存在的用户：失败（且不能抛NPE）
    @Test
    public void loginFailWithUnknownUser(){
        Assertions.assertFalse(virtualHost.login("nobody","123"),"不存在的用户应登录失败");
    }

    //null凭证：失败且不能NPE（防御userMap.get(null)空指针）
    @Test
    public void loginFailWithNullCredentials(){
        Assertions.assertFalse(virtualHost.login(null,null));
        Assertions.assertFalse(virtualHost.login("guest",null));
        Assertions.assertFalse(virtualHost.login(null,"guest"));
    }

    //注册成功后，用注册时的凭证可以登录；重复用户名注册被拒
    @Test
    public void registerThenLogin(){
        Assertions.assertTrue(virtualHost.register("zhangsan","123456"),"首次注册应成功");
        Assertions.assertTrue(virtualHost.login("zhangsan","123456"),"注册凭证应能登录");
        Assertions.assertFalse(virtualHost.register("zhangsan","other"),"重复用户名应注册失败");
        Assertions.assertFalse(virtualHost.login("zhangsan","wrong"),"注册用户的错误密码应登录失败");
    }

    //模拟服务器重启（重建VirtualHost走recovery）：注册过的用户仍能登录
    @Test
    public void userSurvivesRestart(){
        Assertions.assertTrue(virtualHost.register("lisi","abcdef"));
        //重建虚拟主机 == 服务器重启后从硬盘恢复内存
        VirtualHost restarted=new VirtualHost("default");
        Assertions.assertTrue(restarted.login("lisi","abcdef"),"重启后用户数据应被恢复，仍可登录");
        Assertions.assertTrue(restarted.login("guest","guest"),"重启后默认guest账户仍可登录");
    }
}
