package org.example.messagequeuepromax.mqserver.core;

/**
 * 用户管理+验证
 */

public class UserInfo {
    //用户名设置为“主键”不允许用户名重复w
    private String userName;
    private String password;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
