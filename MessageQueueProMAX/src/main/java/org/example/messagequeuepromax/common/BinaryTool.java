package org.example.messagequeuepromax.common;

import java.io.*;

public class BinaryTool {

    //序列化的方法，将各种类型对象转换为二进制
    public static byte[] toByte(Object object){
        if(object==null){
            System.out.println("[BinaryTool] 该对象为空，不能进行序列化");
            return null;
        }
        try(ByteArrayOutputStream byteArrayOutputStream=new ByteArrayOutputStream()){
            try (ObjectOutputStream outputStream=new ObjectOutputStream(byteArrayOutputStream)){
                //调用写入
                outputStream.writeObject(object);
            }
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //反序列化方法，将二进制数据转化为对象
    public static Object toObject(byte[] bytes){
        Object o=null;
        if(bytes==null){
            System.out.println("[BinaryTool] 该二进制数据为空，不能进行序列化");
            return null;
        }
        try (ByteArrayInputStream byteArrayInputStream=new ByteArrayInputStream(bytes)){
            try (ObjectInputStream inputStream=new ObjectInputStream(byteArrayInputStream)){
                o = inputStream.readObject();
            }
            return o;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
