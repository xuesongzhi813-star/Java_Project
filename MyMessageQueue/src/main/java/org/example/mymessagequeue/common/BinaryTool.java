package org.example.mymessagequeue.common;

import org.example.mymessagequeue.mqserver.coreentity.Message;
import org.example.mymessagequeue.mqserver.coreentity.MessageQueue;

import java.io.*;

public class BinaryTool {
    //关于序列化的一个配置
    private static final long serialVersionUID=1L;

    /**
     * 序列化对象：基本实现与IO读写模板类似
     * @param message
     * @return
     */
    public static byte[] toByte(Object message) throws IOException {
        //先创建一个变长字节数组的流
        try(ByteArrayOutputStream byteArrayOutputStream=new ByteArrayOutputStream()){
            try(ObjectOutputStream outputStream=new ObjectOutputStream(byteArrayOutputStream)) {
                //调用方法写入变长字节数组
                outputStream.writeObject(message);
            }
            //转成字节数组
            return byteArrayOutputStream.toByteArray();
        }
    }

    /**
     * 反序列化二进制数据
     */
    public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
        Object o=null;
        try (ByteArrayInputStream byteArrayInputStream=new ByteArrayInputStream(bytes)){
            try (ObjectInputStream inputStream=new ObjectInputStream(byteArrayInputStream)){
                o = inputStream.readObject();
            }
        }
        return o;
    }
}
