package org.example.messagequeuepromax.mqserver.core;

import org.example.messagequeuepromax.mqserver.VirtualHost;
import org.example.messagequeuepromax.mqserver.datacenter.DiskDataCenter;
import org.example.messagequeuepromax.mqserver.datacenter.MemoryDataCenter;

/**
 * 消息处置执行器：全虚拟主机内，"消息结束当前生命周期"的唯一出口
 *
 * 之前同样的删除/重入队逻辑散落在 5 处（ConsumerManager 的 autoAck 与投递失败分支、
 * VirtualHost 的 basicAck/basicReject/requeueOnDisconnect），行为容易漂移；
 * 收敛后所有路径共享同一份执行逻辑，将来扩展死信队列只需填充 routeToDeadLetter。
 *
 * 本类只负责"执行"，不负责"决策"——调用方根据 requeue 开关/队列配置/重试次数
 * 先得出 Disposition，再交由 dispose 统一落地。
 */
public class MessageDisposer {
    //所属虚拟主机：消息数据（内存/硬盘）与消费者管理器都从它获取
    private final VirtualHost virtualHost;
    private final MemoryDataCenter memoryDataCenter;
    private final DiskDataCenter diskDataCenter;

    public MessageDisposer(VirtualHost virtualHost) {
        this.virtualHost = virtualHost;
        this.memoryDataCenter = virtualHost.getMemoryDataCenter();
        this.diskDataCenter = virtualHost.getDiskDataCenter();
    }

    /**
     * 唯一出口：按决策结果处置消息
     * @param disposition 处置决策（由调用方得出）
     * @param queue       消息当前所属队列
     * @param message     待处置消息（应为服务端内存中的权威对象，而非客户端序列化副本）
     * @param reason      死信原因（仅 DEAD_LETTER 时有意义，其余传 null）
     * @return 处置是否成功
     */
    public boolean dispose(Disposition disposition, MessageQueue queue, Message message, DeadLetterReason reason) {
        if (queue == null || message == null) {
            System.out.println("[MessageDisposer] 队列或消息为空，无法处置:messageId:" + (message == null ? "null" : message.getMessageId()));
            return false;
        }
        switch (disposition) {
            case ACK:
                purgeMessage(queue, message);
                System.out.println("[MessageDisposer] 消息已确认消费:queueName:" + queue.getName()
                        + ",messageId:" + message.getMessageId());
                return true;
            case REQUEUE:
                requeue(queue, message);
                return true;
            case DEAD_LETTER:
                return routeToDeadLetter(queue, message, reason);
            case DISCARD:
                purgeMessage(queue, message);
                System.out.println("[MessageDisposer] 消息已被丢弃:queueName:" + queue.getName()
                        + ",messageId:" + message.getMessageId());
                return true;
            default:
                System.out.println("[MessageDisposer] 未知的处置类型:" + disposition);
                return false;
        }
    }

    /**
     * REQUEUE：从"未确认"摘出，重投计数+1，放回队头，立刻通知消费
     */
    private void requeue(MessageQueue queue, Message message) {
        //摘出未确认记录 + 清理 consumerTag 追踪（否则消费者断开时会对已 requeue 的消息重复处理）
        memoryDataCenter.deleteUnAckMessage(queue.getName(), message.getMessageId());
        virtualHost.removeConsumerUnAck(message.getMessageId());
        //重投计数+1：断连/投递失败/消费者拒绝，本质都是"没消费成，再投一次"，统一计数才能兜住所有死循环场景
        message.setDeliveryCount(message.getDeliveryCount() + 1);
        try {
            memoryDataCenter.requeueMessage(queue, message);
        } catch (Exception e) {
            System.out.println("[MessageDisposer] 消息重新入队失败:queueName:" + queue.getName()
                    + ",messageId:" + message.getMessageId());
            return;
        }
        System.out.println("[MessageDisposer] 消息重新入队:queueName:" + queue.getName()
                + ",messageId:" + message.getMessageId() + ",deliveryCount:" + message.getDeliveryCount());
        //通知消费：让放回的消息尽快被（其他）消费者消费掉
        try {
            virtualHost.getConsumerManager().notifyConsumer(queue.getName());
        } catch (InterruptedException e) {
            System.out.println("[MessageDisposer] 通知消费被中断:queueName:" + queue.getName());
        }
    }

    /**
     * 彻底删除消息：未确认记录 + consumerTag追踪 + 内存消息表 + 硬盘（仅持久化消息）
     */
    private void purgeMessage(MessageQueue queue, Message message) {
        memoryDataCenter.deleteUnAckMessage(queue.getName(), message.getMessageId());
        virtualHost.removeConsumerUnAck(message.getMessageId());
        try {
            memoryDataCenter.deleteMessageById(message.getMessageId());
        } catch (Exception e) {
            //消息可能已被并发路径删除，幂等处理
        }
        if (message.getDurable()) {
            try {
                diskDataCenter.deleteMessage(queue, message);
            } catch (Exception e) {
                System.out.println("[MessageDisposer] 硬盘消息删除失败:queueName:" + queue.getName()
                        + ",messageId:" + message.getMessageId());
            }
        }
    }

    /**
     * 死信路由 —— 本期为占位实现，死信队列专项时填充
     *
     * 占位语义：
     *  - 队列未配置 x-dead-letter-exchange：降级为 DISCARD（与 RabbitMQ 行为一致）
     *  - 已配置：同样先安全丢弃（打印日志标记），转投逻辑（查DLX->路由->sendMessage->通知消费）下一期实现
     *
     * 将来实现时本方法体替换为：
     *  1. 从原队列删干净（复用 purgeMessage 的删除部分，但不删 messageMap——转投目标还要用）
     *  2. 附记死信元信息（reason/原队列名/时间戳），保留原 messageId 不重新生成，保证可追溯
     *  3. routingKey 取队列 arguments 的 x-dead-letter-routing-key，缺省用原消息 routingKey
     *  4. 复用现有路由：查死信交换机 -> 按 DIRECT/FANOUT/TOPIC 走 Router 匹配绑定 -> sendMessage 到死信队列
     *  5. 通知死信队列消费
     */
    private boolean routeToDeadLetter(MessageQueue queue, Message message, DeadLetterReason reason) {
        Object dlx = queue.getArguments("x-dead-letter-exchange");
        purgeMessage(queue, message);
        if (dlx == null) {
            System.out.println("[MessageDisposer] 队列未配置死信交换机，消息降级丢弃:queueName:" + queue.getName()
                    + ",messageId:" + message.getMessageId() + ",reason:" + reason);
        } else {
            System.out.println("[MessageDisposer] [占位]死信转投尚未实现，消息暂被丢弃:queueName:" + queue.getName()
                    + ",dlx:" + dlx + ",messageId:" + message.getMessageId() + ",reason:" + reason);
        }
        return true;
    }
}
