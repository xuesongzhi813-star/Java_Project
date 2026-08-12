package org.example.messagequeuepromax.mqserver.datacenter;

import org.example.messagequeuepromax.common.BinaryTool;
import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.core.Message;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

/**
 * 本类负责管理“存储在硬盘上的消息文件”
 * 数据文件格式：消息长度（用固定四字节二进制位来表示）+序列化的Message对象
 * 统计文件格式：total \t effect
 */
public class MessageFileManager {
    public void init() {

    }

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
    private String getDirectPath(String queueName){
        return "./data/"+queueName;
    }

    //获取消息数据文件的路径
    private String getDataPath(String queueName){
        return "./data/"+queueName+"/queue_data.txt";
    }

    //获取消息统计文件的路径
    private String getStatPath(String queueName){
        return "./data/"+queueName+"queue_stat.txt";
    }

    /**
     * 对“统计文件”的读写操作
     */
    //对“统计文件”读操作
    private Stat readStat(String queueName) {
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
    private void writeStat(String queueName,Stat stat) {
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

    //创建指定队列对应的消息文件
    public void createMessageFile(String queueName) throws mqException, IOException {
        File file=new File(getDirectPath(queueName));
        //检查队列目录是否存在
        if(file.exists()){
            //如果存在则不创建
            throw new mqException("该文件夹已经存在:"+queueName);
        }
        file.mkdirs();
        //检查数据文件是否存在
        file=new File(getDataPath(queueName));
        if(file.exists()){
            throw new mqException("该数据文件已经存在:"+queueName+"/queue_data.txt");
        }
        file.createNewFile();
        //检查统计文件是否存在
        file=new File(getStatPath(queueName));
        if(file.exists()){
            throw new mqException("该统计文件已经存在:"+queueName+"/queue_stat.txt");
        }
        file.createNewFile();
        //初始化统计文件
        Stat stat=new Stat();
        stat.total=0;
        stat.effect=0;
        writeStat(queueName,stat);
        System.out.println("[MessageFileManager] 创建成功文件夹及文件:"+queueName);
    }

    //检查目录和消息文件是否存在
    public boolean checkExists(String queueName){
        //文件夹+两个文件存在，才能证明存在，因为创造时是三个一起创造的
        boolean o1=false;
        boolean o2=false;
        boolean o3=false;
        File file=new File(getDirectPath(queueName));
        if(!file.exists()){
            System.out.println("[MessageFileManager] 队列文件夹不存在:"+queueName);
            return false;
        }
        o1=true;
        file=new File(getDataPath(queueName));
        if(!file.exists()){
            System.out.println("[MessageFileManager] 消息数据文件不存在:"+queueName+"/queue_data.txt");
            return false;
        }
        o2=true;
        file=new File(getStatPath(queueName));
        if(!file.exists()){
            System.out.println("[MessageFileManager] 消息统计文件不存在:"+queueName);
            return false;
        }
        o3=true;
        if(o1 && o2 && o3){
            return true;
        }
        return false;
    }

    //删除指定队列的文件夹和消息文件
    public void deleteAllMessage(String queueName) throws mqException {
        //先删除文件，再删除文件夹
        File file=new File(getDataPath(queueName));
        boolean ok = file.delete();
        if(!ok){
            throw new mqException("[MessageFileManager] 删除数据文件失败:/"+queueName+"/queue_data.txt");
        }
        file=new File(getStatPath(queueName));
        ok= file.delete();
        if(!ok){
            throw new mqException("[MessageFileManager] 删除统计文件失败:/"+queueName+"/queue_stat.txt");
        }
        file=new File(getDirectPath(queueName));
        ok= file.delete();
        if(!ok){
            throw new mqException("[MessageFileManager] 删除队列文件夹失败:/"+queueName);
        }
        System.out.println("[MessageFileManager] 指定队列文件夹及文件删除成功:"+queueName);
    }
    /**
     * 消息的文件操作：
     * 1.将消息写入文件
     * 2.展示文件中有效消息
     * 3.删除文件中的指定消息（逻辑删除）
     */
    //将消息写入文件
    //因为要使用加锁操作，因此参数使用“队列对象”
    public void writeMessage(MessageQueue queue, Message message) throws mqException, IOException {
        //判断队列对应文件是否存在
        boolean ok = checkExists(queue.getName());
        if(!ok){
            throw new mqException("[MessageFileManager] 指定的队列文件夹/文件不存在:"+queue.getName());
        }
        //写入操作，会出现线程安全问题，造成消息的offset可能不准确，采取加锁
        synchronized (queue) {
            //将消息序列化
            byte[] payload = BinaryTool.toByte(message);
            int length = payload.length;
            //先获取到数据文件大小，从而设置“写入消息”在其中的offset，便于查询获取
            File file=new File(getDataPath(queue.getName()));
            //Begin在文件长度+4（4是存储本次消息序列化后信息长度）
            //Begin和End要指向标明“消息对象序列化”的位置，因此从长度后开始计算
            message.setOffsetBegin((int) (file.length()+4));
            message.setOffsetEnd((int) (file.length()+4+length));
            //写入数据文件
            //一定要使用true-->“追加写”，否则无论写多少数据，都是1
            try (OutputStream outputStream=new FileOutputStream(file,true)){
                //写入二进制文件，使用DataOutputStream
                try (DataOutputStream dataOutputStream=new DataOutputStream(outputStream)){
                    //先写入本段消息长度
                    dataOutputStream.writeInt(length);
                    //写入消息数据
                    dataOutputStream.write(payload);
                    dataOutputStream.flush();
                }
            }
            //更新统计数据
            Stat stat = readStat(queue.getName());
            stat.total+=1;
            stat.effect+=1;
            writeStat(queue.getName(),stat);
            System.out.println("[MessageFileManager] 消息写入文件成功，queue:"+queue.getName()+",message:"+message.getMessageId());
        }
    }

    //查询所有有效消息
    //查询操作，不涉及线程安全问题，正常使用queueName查询即可
    public LinkedList<Message> loadAllMessage(String queueName) throws mqException, IOException {
        //检查查询的文件夹是否存在
        if(!checkExists(queueName)){
            throw new mqException("[MessageFileManager] 查询的文件不存在:"+queueName);
        }
        File file=new File(getDataPath(queueName));
        LinkedList<Message> messages=new LinkedList<>();
        //定义一个在文件中的“读取光标”位置
        int currentLocation=0;
        //进行查询操作
        try (InputStream inputStream=new FileInputStream(file)){
            //读取二进制文件，流使用DataInputStream
            try (DataInputStream dataInputStream=new DataInputStream(inputStream)){
                while (true){
                    try {
                        //先读取到消息长度
                        int messageLength = dataInputStream.readInt();
                        byte[] body=new byte[messageLength];
                        //读取消息正文
                        int read = dataInputStream.read(body);
                        if(read!=messageLength){
                            throw new mqException("[MessageFileManager] 消息读取有误:"+queueName);
                        }
                        //反序列化消息
                        Message message = (Message) BinaryTool.toObject(body);
                        //无效数据跳过，不读取
                        if(message.getIsValid()==0x0){
                            //更新“读取光标”位置
                            currentLocation+=(4+messageLength);
                            continue;
                        }
                        //有效数据则加入链表，先设置好offset属性
                        message.setOffsetBegin(currentLocation+4);
                        message.setOffsetEnd(currentLocation+4+messageLength);
                        //更新“读取光标”位置
                        currentLocation+=(4+messageLength);
                        messages.add(message);
                    }catch (EOFException e){
                        //触发EOF异常，读取结束
                        System.out.println("[MessageFileManager] 有效消息读取完成:"+queueName);
                        break;
                    }
                }
            }
        }
        return messages;
    }

    //删除指定的消息（逻辑删除）
    public void deleteMessage(MessageQueue queue,Message message) throws mqException, IOException {
        //判断文件是否存在
        if(!checkExists(queue.getName())){
            throw new mqException("[MessageFileManager] 文件夹及文件不存在:"+queue.getName());
        }
        synchronized (queue){
        File file=new File(getDataPath(queue.getName()));
            //想要在二进制文件中“随机访问下标删除”，采取RandomAccessFile
            try (RandomAccessFile randomAccessFile=new RandomAccessFile(file,"rw")){
                //先移动到要删除的消息位置
                randomAccessFile.seek(message.getOffsetBegin());
                //读取
                byte[] body=new byte[message.getOffsetEnd()- message.getOffsetBegin()];
                int read = randomAccessFile.read(body);
                //判断长度
                if(read!=body.length){
                    throw new mqException("[MessageFileManager] 删除文件中，读取文件有误:queue:"+queue.getName()+"message:"+
                            message.getMessageId());
                }
                //反序列化+设置删除标识
                Message messages = (Message) BinaryTool.toObject(body);
                messages.setIsValid((byte) 0x0);
                //重新写入文件，覆盖原消息数据
                randomAccessFile.seek(message.getOffsetBegin());
                byte[] payload = BinaryTool.toByte(messages);
                randomAccessFile.write(payload);
            }
            //更新统计文件，有效文件数-1
            Stat stat = readStat(queue.getName());
            stat.effect-=1;
            writeStat(queue.getName(),stat);
            System.out.println("[MessageFileManager] 删除消息成功:queue:"+queue.getName()+",message:"+message.getMessageId());
        }
    }

    /**
     * 垃圾回收机制的实现：
     * 1.先判定是否满足触发机制要求
     * 2.创建一个新的文件，准备复制
     * 3.垃圾回收机制
     */
    //判断是否触发“垃圾回收机制”
    //触发条件：有效消息数<50%/消息总数>2000
    public boolean isGC(String queueName){
        File file=new File(getStatPath(queueName));
        Stat stat = readStat(queueName);
        if((stat.total>2000) || (stat.effect/stat.total)<0.5){
            return true;
        }
        return false;
    }

    //返回新的复制文件的路径
    public String getNewDataPath(String queueName){
        return "./data/"+queueName+"/queue_new_data.txt";
    }

    //垃圾回收机制
    public void GC(MessageQueue queue) throws IOException, mqException {
//        //先判定是否满足触发条件
//        if(!isGC(queue.getName())){
//            System.out.println("[MessageFileManager]  不满足触发垃圾回收机制的条件:"+queue.getName());
//            return;
//        }
        //先创建一个新文件
        File file=new File(getNewDataPath(queue.getName()));
        //判断这个文件是否存在，不该存在！！！
        if(file.exists()){
            throw new mqException("[MessageFileManager] 新文件不应该存在！！");
        }
        if(!file.createNewFile()){
            throw new mqException("[MessageFileManager] 创建新的复制文件失败:"+queue.getName());
        }
        synchronized (queue){
            //读取旧文件中的有效文件
            List<Message> messages = loadAllMessage(queue.getName());
            //通过读取旧文件内容，写入新文件
            for (Message message:messages){
                try (OutputStream outputStream=new FileOutputStream(file)){
                    try (DataOutputStream dataOutputStream=new DataOutputStream(outputStream)){
                        byte[] payload = BinaryTool.toByte(message);
                        //写入长度
                        dataOutputStream.writeInt(payload.length);
                        //写入消息
                        dataOutputStream.write(payload);
                        dataOutputStream.flush();
                    }
                }
            }
            //全复制到“新文件”，改名覆盖删除老文件
            File oldFile=new File(getDataPath(queue.getName()));
            boolean ok = oldFile.delete();
            if(!ok){
                throw new mqException("[MessageFileManager] 回收新文件覆盖删除失败");
            }
            file.renameTo(oldFile);
            //更新统计数据
            Stat stat = readStat(queue.getName());
            stat.total=stat.effect;
            writeStat(queue.getName(),stat);
            System.out.println("[MessageFileManager] 垃圾回收执行完成:"+queue.getName());
        }
    }
}
