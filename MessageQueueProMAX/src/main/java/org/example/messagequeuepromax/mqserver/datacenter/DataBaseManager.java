package org.example.messagequeuepromax.mqserver.datacenter;

import org.example.messagequeuepromax.MessageQueueProMaxApplication;
import org.example.messagequeuepromax.common.exchangeType;
import org.example.messagequeuepromax.mqserver.core.Binding;
import org.example.messagequeuepromax.mqserver.core.Exchange;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;
import org.example.messagequeuepromax.mqserver.core.UserInfo;
import org.example.messagequeuepromax.mqserver.mapper.DiskMapper;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * 该类负责“统一调用SQL语句”完成数据库存储的功能
 */
public class DataBaseManager {
    //先注入mapper才能调用SQL语句
    private DiskMapper diskMapper;

    /**
     * 通过Spring上下文或手动注入DiskMapper
     */
//    public void setDiskMapper(DiskMapper diskMapper) {
//        this.diskMapper = diskMapper;
//    }

    /**
     * 数据库初始化：
     * 创建data文件夹+创建数据库文件+插入默认交换机
     */
    public void init(){
        //先注入mapper依赖，才能调用SQL
        diskMapper=MessageQueueProMaxApplication.context.getBean(DiskMapper.class);
        if (diskMapper == null) {
            diskMapper = MessageQueueProMaxApplication.context.getBean(DiskMapper.class);
        }
        //先判断data文件夹是否存在
        File file=new File("./data");
        if(!file.exists()){
            //如果不存在则先创建文件夹
            file.mkdirs();
        }
        //存在则进行数据库的创建
        createDataBase();
        //插入默认交换机
        Exchange exchange=createDefault();
        insertExchange(exchange);
        System.out.println("[DataBaseManager] 默认交换机插入成功");
        System.out.println("[DataBaseManager] 数据库初始化完成");
    }

    private void createDataBase() {
        diskMapper.createExchangeTable();
        diskMapper.createQueueTable();
        diskMapper.createBindingTable();
        diskMapper.createUserTable();
    }

    private Exchange createDefault(){
        Exchange exchange=new Exchange();
        exchange.setName("defaultExchange");
        exchange.setExchangeType(exchangeType.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        return exchange;
    }

    /**
     * 对增，删四个表数据的SQL语句进行封装成方法
     */
    public void insertExchange(Exchange exchange){
        diskMapper.insertExchange(exchange);
    }

    public void insertQueue(MessageQueue queue){
        diskMapper.insertMessageQueue(queue);
    }

    public void insertBinding(Binding binding){
        diskMapper.insertBinding(binding);
    }

    public void insertUser(UserInfo userInfo){ diskMapper.insertUserInfo(userInfo);}

    public void deleteExchange(String name){diskMapper.deleteExchange(name);
    }
    public void deleteQueue(String name){
        diskMapper.deleteMessageQueue(name);
    }

    public void deleteBinding(Binding binding){
        diskMapper.deleteBinding(binding);
    }

    public void deleteUser(String userName) {diskMapper.deleteUserInfo(userName);}

    /**
     * 对查三个表的全部数据的SQL语句进行封装成方法
     */
    public List<Exchange> selectAllExchange(){
        return diskMapper.selectAllExchange();
    }

    public List<MessageQueue> selectAllQueue(){
        return diskMapper.selectAllQueue();
    }

    public List<Binding> selectAllBinding(){
        return diskMapper.selectAllBinding();
    }

    public List<UserInfo> selectAllUser(){ return diskMapper.selectAllUser(); }

    /**
     * 删除所有文件 :删除数据库文件即可
     */
    public void deleteAll(){
        File file=new File("./data/meta.db");
        //先判断数据库文件是否存在
        if(!file.exists()){
            System.out.println("[DataBaseManager] 数据库文件不存在:"+file.getName());
            return;
        }
        //存在则进行删除操作
        if(file.delete()){
            System.out.println("[DataBaseManager] 数据库文件删除成功");
            return;
        }
        System.out.println("[DataBaseManager] 数据库文件删除失败");
    }
}
