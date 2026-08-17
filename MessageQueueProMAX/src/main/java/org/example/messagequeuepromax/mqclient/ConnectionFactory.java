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

    //创建连接
    public Connection createConnection() throws IOException {
        Connection connection=new Connection(host,port);
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
}
