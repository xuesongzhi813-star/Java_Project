package org.example.mymessagequeue.mqserver.datacenter;

import org.example.mymessagequeue.MyMessageQueueApplication;
import org.example.mymessagequeue.mqserver.coreentity.Binding;
import org.example.mymessagequeue.mqserver.coreentity.Exchange;
import org.example.mymessagequeue.mqserver.coreentity.MessageQueue;
import org.example.mymessagequeue.mqserver.coreentity.exchangetype;
import org.example.mymessagequeue.mqserver.mapper.MetaMapper;

import java.io.File;
import java.io.FileFilter;
import java.util.List;

/**
 * 整合数据库操作，统一调度使用SQL语句
 */
public class DataBaseManager {
    //通过Spring上下文注入MetaMapper
    private MetaMapper metaMapper;
    /**
     * 初始化数据库：init+createDefault+insertDefault+createTable
     * 创建数据表+插入默认数据
     * 1.创建数据表：若在已部署过的服务器上不必再创建（已经有备份）；若部署在新服务器上进行创建
     * 2.插入默认数据：在创建表的前提下，插入一个匿名交换机，类别为DIRECT
     */
    public void init(){
        //注入实现SQL语句的对象
        metaMapper= MyMessageQueueApplication.context.getBean(MetaMapper.class);
        //判断是否已经创建过数据库
        File file=new File("./data/meta.db");
        if(file.exists()){
            //不再进行创建
            System.out.println("[dataBaseManager]：数据表已经存在");
            return;
        }
        else {
            //创建表
            createTable();
            System.out.println("[dataBaseManager]：创建数据表成功");
            //插入默认数据
            Exchange exchange=createDefault();
            insertDefault(exchange);
            System.out.println("[dataBaseManager]：插入匿名交换机成功");
        }
    }

    private Exchange createDefault() {
        Exchange exchange=new Exchange();
        exchange.setName("");
        exchange.setExchageType(exchangetype.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(true);
        return exchange;
    }

    private void insertDefault(Exchange exchange){
        metaMapper.insertExchange(exchange);
    }

    private void createTable() {
        metaMapper.createExchangeTable();
        metaMapper.createMessageQueueTable();
        metaMapper.createBindingTable();
    }

    /**
     * 对增，删三个表数据的SQL语句进行封装成方法
     */
    public void insertExchange(Exchange exchange){
        metaMapper.insertExchange(exchange);
    }

    public void insertQueue(MessageQueue queue){
        metaMapper.insertMessageQueue(queue);
    }

    public void insertBinding(Binding binding){
        metaMapper.insertBinding(binding);
    }

    public void deleteExchange(String name){
        metaMapper.deleteExchange(name);
    }

    public void deleteQueue(String name){
        metaMapper.deleteMessageQueue(name);
    }

    public void deleteBinding(Binding binding){
        metaMapper.deleteBinding(binding);
    }

    /**
     * 对查三个表的全部数据的SQL语句进行封装成方法
     */
    public List<Exchange> selectAllExchange(){
        return metaMapper.selectAllExchange();
    }

    public List<MessageQueue> selectAllQueue(){
        return metaMapper.selectAllQueue();
    }

    public List<Binding> selectAllBinding(){
        return metaMapper.selectAllBinding();
    }

    /**
     * 删除所有信息：删除文件
     */
    public void deleteAll() {
        File file=new File("./data/meta.db");
        boolean delete = file.delete();
        if(delete){
            System.out.println("[dataBaseManager]：文件删除成功！");
        }
        else {
            System.out.println("[dataBaseManager]：文件删除失败！");
        }
    }
}
