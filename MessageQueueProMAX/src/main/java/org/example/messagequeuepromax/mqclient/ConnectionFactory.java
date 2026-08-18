package org.example.messagequeuepromax.mqclient;

import java.io.IOException;

/**
 * 连接工厂类：
 * 持有服务器地址，创建TCP连接对象
 */
public class ConnectionFactory {
    //服务器ip
    private String host;
    //服务器端口号
    private int port;
    //用户信息
    //默认guest/guest：客户端不设置凭证时也能登录（服务器init时种子了guest账户），避免null凭证发到服务器触发NPE
    private String userName="guest";
    private String password="guest";

    //创建连接
    public Connection createConnection() throws IOException {
        Connection connection=new Connection(host,port,userName,password);
        return connection;
    }
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String passwordl) {
        this.password = passwordl;
    }
}
