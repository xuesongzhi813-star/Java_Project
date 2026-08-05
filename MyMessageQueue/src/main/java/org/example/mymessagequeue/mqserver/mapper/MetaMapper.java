package org.example.mymessagequeue.mqserver.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.example.mymessagequeue.mqserver.coreentity.Binding;
import org.example.mymessagequeue.mqserver.coreentity.Exchange;
import org.example.mymessagequeue.mqserver.coreentity.MessageQueue;

import java.util.List;

/**
 * 只负责SQL语句的实现，不管如何使用
 */
@Mapper
public interface MetaMapper {
    //创建核心类的数据表（库在调用时，Mybatis会创造库，其实也就是meta.db文件）
    void createExchangeTable();
    void createMessageQueueTable();
    void createBindingTable();

    //三个数据库的插入数据，删除方法
    void insertExchange(Exchange exchange);
    //通过主键name删除交换机
    void deleteExchange(String name);
    void insertMessageQueue(MessageQueue queue);
    //通过主键name删除队列
    void deleteMessageQueue(String name);
    void insertBinding(Binding binding);
    //通过binding对象删除绑定（需要交换机的主键和队列的主键）
    void deleteBinding(Binding binding);

    //三个数据库的查询全部数据方法
    List<Exchange> selectAllExchange();
    List<MessageQueue> selectAllQueue();
    List<Binding> selectAllBinding();
}
