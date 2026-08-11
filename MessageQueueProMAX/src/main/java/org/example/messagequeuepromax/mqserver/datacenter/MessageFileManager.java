package org.example.messagequeuepromax.mqserver.datacenter;

import java.io.*;
import java.util.Scanner;

/**
 * 本类负责管理“存储在硬盘上的消息文件”
 * 数据文件格式：消息长度（用固定四字节二进制位来表示）+序列化的Message对象
 * 统计文件格式：total \t effect
 */
public class MessageFileManager {
    //消息文件的统计文件，创建为静态类
    public static class Stat{
        //文件内消息的总数
        public int total;
        //文件内有效消息的数量
        public int effect;
    }

    /**
     * 因为“消息”是通过文件的形式存储，因此要操作消息文件，就要获得“绝对路径”才能进行“文件操作”
     * 存储方式：data文件夹->队列名文件夹->每个队列对应的“消息数据文件”+“消息统计文件”
     * 为什么这么设计？“消息”发送给“队列”，再由“队列”发给“消费者”，取走之前“消息”都是依附于“队列”的，因此持久化存储上，将二者绑定
     */
    //获取队列名文件夹的路径
    public String getDirectPath(String queueName){
        return "./data/"+queueName;
    }

    //获取消息数据文件的路径
    public String getDataPath(String queueName){
        return "./data/"+queueName+"/queue_data.txt";
    }

    //获取消息统计文件的路径
    public String getStatPath(String queueName){
        return "./data/"+queueName+"queue_stat.txt";
    }

    /**
     * 对“统计文件”的读写操作
     */
    //对“统计文件”读操作
    public Stat readStat(String queueName) {
        File file=new File(getStatPath(queueName));
        //检查文件是否存在
        if(!file.exists()){
            System.out.println("[MessageFileManager] 该队列名对应的“统计文件”并不存在！");
            return null;
        }
        Stat stat=new Stat();
        try (InputStream inputStream=new FileInputStream(file)){
                Scanner scanner=new Scanner(inputStream);
                //先读total
                stat.total=scanner.nextInt();
                //再读effect
                stat.effect=scanner.nextInt();
        }  catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stat;
    }

    //对“统计文件”写操作
    public void writeStat(String queueName,Stat stat) {
        File file=new File(getStatPath(queueName));
        if(!file.exists()){
            System.out.println("[MessageFileManager] 该队列名对应的“统计文件”并不存在！");
            return;
        }
        try (OutputStream outputStream=new FileOutputStream(file)){
            PrintWriter printWriter=new PrintWriter(outputStream);
            printWriter.write(stat.total+"\t"+stat.effect);
            printWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
