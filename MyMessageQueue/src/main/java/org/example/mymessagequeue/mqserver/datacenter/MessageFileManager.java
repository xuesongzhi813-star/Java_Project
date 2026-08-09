package org.example.mymessagequeue.mqserver.datacenter;

import org.example.mymessagequeue.common.BinaryTool;
import org.example.mymessagequeue.common.mqException;
import org.example.mymessagequeue.mqserver.coreentity.Message;
import org.example.mymessagequeue.mqserver.coreentity.MessageQueue;

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;

/**
 * 管理操作“消息文件”（对硬盘上进行操作）
 */
public class MessageFileManager {

    public void init() {

    }

    /**
     * 对应两个存储文件中的“消息统计”文件
     */
    public static class Stat {
        public int total;
        public int effect;
    }

    /**
     * 基于目录结构管理消息的方法：
     * 1.查询当前在哪个“队列目录”的查询路径方法
     * 2.查询指定队列目录中“数据文件”的完整路径方法
     * 3.查询指定队列目录中“统计消息文件”的完整路径方法
     */
    // 获取指定队列目录中“消息文件路径”-->在哪个队列
    private String findQueueDirPath(String queueName) {
        return "./data/" + queueName;
    }

    //获取指定队列目录中“数据文件”的完整路径
    private String findQueueDataPath(String queueName) {
        return "./data/" + queueName + "/queue_data.txt";
    }

    //获取指定队列目录中“统计消息文件”的完整路径
    private String findQueueStatPath(String queueName) {
        return "./data/" + queueName + "/queue_stat.txt";
    }

    /**
     * 关于“统计消息文件”的读写方法
     */
    //统计文件的读方法
    private Stat readStat(String queueName) {
        File statfile = new File(findQueueStatPath(queueName));
        Stat stat = new Stat();
        //文件流读写的方法
        try (InputStream inputStream = new FileInputStream(statfile)) {
            Scanner scanner = new Scanner(inputStream);
            //按原静态类中属性顺序读取
            stat.total = scanner.nextInt();
            stat.effect = scanner.nextInt();
            return stat;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //统计文件的写方法
    private void writeStat(String queueName, Stat stat) {
        File file = new File(findQueueStatPath(queueName));
        try (OutputStream outputStream = new FileOutputStream(file)) {
            PrintWriter printWriter = new PrintWriter(outputStream);
            printWriter.write(stat.total + "\t" + stat.effect);
            printWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建队列对应文件和目录
     */
    public void createMkdir(String queueName) throws IOException {
        //先创建队列的目录（mkdirs 确保父目录一并创建）
        File file = new File(findQueueDirPath(queueName));
        if (!file.exists()) {
            boolean ok = file.mkdirs();
            if (!ok) {
                throw new IOException("无法创建队列目录: " + file.getAbsolutePath());
            }
            System.out.println("[MessageFileManager]：队列目录创建成功");
        } else {
            System.out.println("[MessageFileManager]：当前队列目录已存在！");
        }
        //创建队列的数据文件
        file = new File(findQueueDataPath(queueName));
        if (!file.exists()) {
            boolean ok = file.createNewFile();
            if (!ok) {
                throw new IOException("无法创建数据文件: " + file.getAbsolutePath());
            }
            System.out.println("[MessageFileManager]：数据文件创建成功");
        } else {
            System.out.println("[MessageFileManager]：当前数据文件已存在！");
        }
        //创建队列的统计文件
        file = new File(findQueueStatPath(queueName));
        if (!file.exists()) {
            boolean ok = file.createNewFile();
            if (!ok) {
                throw new IOException("无法创建统计文件: " + file.getAbsolutePath());
            }
            System.out.println("[MessageFileManager]：统计文件创建成功");
        } else {
            System.out.println("[MessageFileManager]：统计文件已存在！");
        }
        //统计文件初始化
        Stat stat = new Stat();
        stat.total = 0;
        stat.effect = 0;
        writeStat(queueName, stat);
    }

    /**
     * 删除队列目录和附属文件
     */
    public void deleteMkdirAndFile(String queueName) throws IOException {
        //删除数据文件（文件不存在时跳过，不抛异常）
        File file = new File(findQueueDataPath(queueName));
        if (file.exists()) {
            boolean ok = file.delete();
            if (!ok) {
                throw new IOException("无法删除数据文件: " + file.getAbsolutePath());
            }
        }
        //删除统计文件
        file = new File(findQueueStatPath(queueName));
        if (file.exists()) {
            boolean ok = file.delete();
            if (!ok) {
                throw new IOException("无法删除统计文件: " + file.getAbsolutePath());
            }
        }
        //删除目录
        file = new File(findQueueDirPath(queueName));
        if (file.exists()) {
            boolean ok = file.delete();
            if (!ok) {
                throw new IOException("无法删除队列目录: " + file.getAbsolutePath());
            }
        }
    }

    /**
     * 检查目录，文件是否存在
     */
    public Boolean fileExists(String queueName) throws IOException {
        Boolean d1 = null;
        Boolean d2 = null;
        Boolean d3 = null;
        //查询数据文件
        File file = new File(findQueueDataPath(queueName));
        d1 = file.exists();
        //查询统计文件
        file = new File(findQueueStatPath(queueName));
        d2 = file.exists();
        //查询目录
        file = new File(findQueueDirPath(queueName));
        d3 = file.exists();
        if (d1 == true || d2 == true || d3 == true) {
            return true;
        }
        return false;
    }

    /**
     * 注：方法中需要“加锁”，参数才是“队列对象”
     * 消息写入文件:
     */
    public void sendMessage(MessageQueue queue, Message message) throws mqException, IOException {
        //检查写入的队列是否存在
        Boolean ok = fileExists(queue.getName());
        if (ok.equals(false)) {
            throw new mqException("当前目录以及文件并不存在");
        }
        synchronized (queue) {
            //进行序列化
            byte[] aByte = BinaryTool.toByte(message);
            //获取原数据长度，推导写入消息的offsetBegin,offsetEnd
            File file = new File(findQueueDataPath(queue.getName()));
            message.setOffsetBegin((int) (file.length() + 4));
            message.setOffsetEnd((int) (file.length() + 4 + aByte.length));
            //将消息写入数据文件
            try (OutputStream outputStream = new FileOutputStream(file, true)) {
                try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                    //先写入“消息”的长度，按四个字节写入
                    dataOutputStream.writeInt(aByte.length);
                    //写入“消息”主体数据
                    dataOutputStream.write(aByte);
                }
            }
            //更新统计文件（先读取现有值再累加，避免覆盖）
            Stat stat = readStat(queue.getName());
            stat.total += 1;
            stat.effect += 1;
            writeStat(queue.getName(), stat);
        }
    }

    /**
     * 删除文件中的“消息”：
     * 采取逻辑删除，置数isValid=0
     */
    public void deleteMessage(MessageQueue queue, Message message) throws IOException, ClassNotFoundException {
        synchronized (queue) {
            File file = new File(findQueueDataPath(queue.getName()));
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
                //先根据offset读取出指定消息
                byte[] bytes = new byte[message.getOffsetEnd() - message.getOffsetBegin()];
                randomAccessFile.seek(message.getOffsetBegin());
                //输出型参数，用bytes去接收返回的数据
                randomAccessFile.read(bytes);
                //读出数据反序列化，还原为对象
                Message m = (Message) BinaryTool.toObject(bytes);
                //设置isValid=0
                m.setIsValid((byte) 0x0);
                //调整光标位置，再重新写入文件
                bytes = BinaryTool.toByte(m);
                randomAccessFile.seek(message.getOffsetBegin());
                randomAccessFile.write(bytes);
            }
            //更新统计文件，有效文件数-1（先读取现有值再递减）
            Stat stat = readStat(queue.getName());
            stat.effect -= 1;
            writeStat(queue.getName(), stat);
        }
    }

    /**
     * 加载文件中所有消息
     */
    public LinkedList<Message> loadAllMessage(String queueName) throws IOException, mqException, ClassNotFoundException {
        LinkedList<Message> linkedList = new LinkedList<>();
        File file = new File(findQueueDataPath(queueName));
        long currentSeek = 0;
        // 将流的创建移到 while 外部，确保连续读取而不是每次从头开始
        try (InputStream inputStream = new FileInputStream(file);
             DataInputStream dataInputStream = new DataInputStream(inputStream)) {
            while (true) {
                try {
                    //先读取消息的长度，4个字节
                    int messageLength = dataInputStream.readInt();
                    //读取消息的字节数组（body）
                    byte[] bytes = new byte[messageLength];
                    //判断实际长度与预期长度是否有不同
                    if (!(messageLength == dataInputStream.read(bytes))) {
                        throw new mqException("文件读取有误!");
                    }
                    //反序列化对象
                    Message message = (Message) BinaryTool.toObject(bytes);
                    //判断是否是有效对象
                    if (message.getIsValid() == 0x0) {
                        //无效也要设置光标位置
                        currentSeek += (4 + messageLength);
                        //无效则跳过
                        continue;
                    }
                    //设置offset
                    message.setOffsetBegin((int) (currentSeek + 4));
                    message.setOffsetEnd((int) (currentSeek + 4 + messageLength));
                    currentSeek += (4 + messageLength);
                    //有效消息加入链表
                    linkedList.add(message);
                } catch (EOFException e) {
                    // 这个 catch 并非真是处理 "异常", 而是处理 "正常" 的业务逻辑. 文件读到末尾, 会被 readInt 抛出该异常.
                    System.out.println("[MessageFileManager] 恢复 Message 数据完成!");
                    break;
                }
            }
        }
        return linkedList;
}

/**
 * 实现垃圾回收机制：判断能否触发+获取新文件路径（要复制的）+垃圾回收的实现
 */
//判断是否进行CG
public boolean isCG(String queueName){
    Stat stat = readStat(queueName);
    if(stat.total>2000||((float)stat.effect/(float)stat.total)<0.5){
        return true;
    }
    return false;
}

//获取新文件路径
    private String findQueueNewData(String queueName){
    return "./data/"+queueName+"/newdata.txt";
    }

    //实现CG机制（垃圾回收机制）
    public void CG(MessageQueue queue) throws IOException, mqException, ClassNotFoundException {
    synchronized (queue){
        long startTime=System.currentTimeMillis();
        //创建一个新文件，准备复制有效文件过去
        File file=new File(findQueueNewData(queue.getName()));
        //
        if (file.exists()){
            throw new mqException("异常，文件不该存在！");
        }
        if (!file.createNewFile()){
            throw new mqException("异常！");
        }
        //旧文件中读取有效对象
        LinkedList<Message> linkedList = loadAllMessage(queue.getName());
        //写入新文件中
        try (OutputStream outputStream=new FileOutputStream(file,true)){
            try (DataOutputStream dataOutputStream=new DataOutputStream(outputStream)){
                //循环写入新文件中
                for(Message list:linkedList){
                    //先读取出源文件中数据
                    byte[] bytes=BinaryTool.toByte(list);
                    //写入新文件
                    dataOutputStream.writeInt(bytes.length);
                    dataOutputStream.write(bytes);
                }
            }
        }

        //删除旧文件
        File oldFile=new File(findQueueDataPath(queue.getName()));
        if(oldFile.delete()){
            //删除成功，重命名新文件
            file.renameTo(oldFile);
        }
        else {
            throw new mqException("");
        }

        //更新有效消息
        Stat stat=new Stat();
        stat.total=linkedList.size();
        stat.effect=linkedList.size();
        writeStat(queue.getName(),stat);
    }
    }
}
