package org.example.mymessagequeue.mqclient;

import java.io.IOException;

public class ConnectionFactory {
    //服务器IP
    private String host;
    //服务器端口号
    private int port;

    //创建一个connection
    public Connection newConnection() throws IOException {
        Connection connection=new Connection(host,port);
        return connection;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
