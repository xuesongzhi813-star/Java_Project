package org.example.messagequeuepromax.common;

import org.springframework.util.DigestUtils;

import javax.lang.model.util.SimpleElementVisitor14;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * 对密码实现加密存储的工具类:
 */
public class Md5Utils {
    /**
     * 加密密码：
     * 思路：
     * 1.生成盐值（用以增加密码的复杂性，防止用户设置密码过于简单）
     * 2.拼接盐值+用户密码
     * 3.对拼接后密码进行加密-->Md5算法加密
     * 4.返回存储“盐值”+“加密后密码”（增加复杂度）
     */
    public static String encrytion(String primaryPassword){
        //生成盐值先
        String salt= UUID.randomUUID().toString().replace("-","");
        //盐值+原始密码-->得到复杂的密码
        String password=salt+primaryPassword;
        //对复杂密码进行Md5加密
        String secretPassword= DigestUtils.md5DigestAsHex(password.getBytes(StandardCharsets.UTF_8));
        //返回存储密码
        return salt+secretPassword;
    }

    /**
     * 解密：
     * 前提：盐值+最终密码，在数据库中存储长度为64，前32位为盐值
     * 思路：
     */
    public static boolean verify(String password,String sqlPassword){
        //先检查密码不能为空
        if(password==null){
            System.out.println("[Md5Utils] 密码不能为空");
            return false;
        }
        //数据库密码为空直接失败（否则下方sqlPassword.length()会空指针）
        if(sqlPassword==null){
            System.out.println("[Md5Utils] 数据库中密码为空");
            return false;
        }
        //判断数据库中密码长度是否正确
        if(sqlPassword.length()!=64){
            System.out.println("[Md5Utils] 数据库中密码长度不正确");
            return false;
        }
        //获取一样的盐值，从而好加密后比较(盐值为前32位)
        String salt=sqlPassword.substring(0,32);
        String s=salt+password;
        String finalPassword=DigestUtils.md5DigestAsHex(s.getBytes(StandardCharsets.UTF_8));
        return sqlPassword.equals(salt+finalPassword);
    }
}
