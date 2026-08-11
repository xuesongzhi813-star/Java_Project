package org.example.messagequeuepromax.mqserver.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.example.messagequeuepromax.mqserver.core.Binding;
import org.example.messagequeuepromax.mqserver.core.Exchange;
import org.example.messagequeuepromax.mqserver.core.MessageQueue;
import org.example.messagequeuepromax.mqserver.core.UserInfo;

import java.util.List;

/**
 * 管理实现好的SQL语句
 */
@Mapper
public interface DiskMapper {
    /**
     * 创建存储在数据库中：交换机，队列，绑定的数据表，用户信息
     */
    void createExchangeTable();
    void createQueueTable();
    void createBindingTable();
    void createUserTable();

    /**
     * 向数据库中插入，删除信息
     */
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
    //用户信息的插入
    void insertUserInfo(UserInfo userInfo);
    //根据主键删除用户
    void deleteUserInfo(String userName);

    //三个数据库的查询全部数据方法
    List<Exchange> selectAllExchange();
    List<MessageQueue> selectAllQueue();
    List<Binding> selectAllBinding();
    List<UserInfo> selectAllUser();
}
