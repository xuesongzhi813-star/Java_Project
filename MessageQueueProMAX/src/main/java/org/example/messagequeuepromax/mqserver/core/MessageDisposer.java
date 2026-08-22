package org.example.messagequeuepromax.mqserver.core;

import org.example.messagequeuepromax.common.mqException;
import org.example.messagequeuepromax.mqserver.VirtualHost;
import org.example.messagequeuepromax.mqserver.datacenter.DiskDataCenter;
import org.example.messagequeuepromax.mqserver.datacenter.MemoryDataCenter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

    //死信元信息在消息头(basicProperties.headers)里的 key：客户端消费死信时由此取回 DeadLetterInfo
    public static final String X_DEATH = "x-death";

    /**
     * 死信路由：把死掉的消息转投到"死信队列"（约定式设计，沿用原有思路）：
     *  - 队列配置 x-death-exchange 指定死信交换机名（键名与决策层 resolveDisposition 统一）
     *  - 死信队列名 = 死信交换机名 + "_queue"（约定配对创建）
     *    （不能用 "-queue"：Router.chechRoutingKey 不允许连字符，死信若被再次经交换机发布会被拦截）
     *  - 未配置死信交换机 / 死信队列未创建：降级为 DISCARD（与 RabbitMQ 行为一致）
     *
     * 转投不走 basicPublish，直接对目标队列 sendMessage，原因：
     *  1. basicPublish 的 messageFactory 会生成新 messageId，破坏死信追溯链（必须保留原 id）
     *  2. basicPublish 的 DIRECT 分支 selectQueue 不补虚拟主机前缀（老问题，Producer1Demo 靠手写前缀绕过）
     * sendMessage 自带"内存+硬盘+通知消费"三件套，转投即完成
     */
    private boolean routeToDeadLetter(MessageQueue queue, Message message, DeadLetterReason reason) {
        //1. 读队列配置的死信交换机（先判空再 toString，顺序不能反；键名统一为 x-death-exchange）
        Object dlx = queue.getArguments("x-death-exchange");
        if (dlx == null) {
            System.out.println("[MessageDisposer] 队列未配置死信交换机，消息降级丢弃:queueName:" + queue.getName()
                    + ",messageId:" + message.getMessageId() + ",reason:" + reason);
            purgeMessage(queue, message);
            return false;
        }
        String dlxName = dlx.toString();

        //2. 按约定推导死信队列名（补虚拟主机前缀才是 queueMap 里的真实 key）
        MessageQueue dlQueue = memoryDataCenter.selectQueue(virtualHost.getVirtualHostName() + dlxName + "_queue");
        if (dlQueue == null) {
            System.out.println("[MessageDisposer] 死信队列未按约定创建(约定名:" + dlxName + "_queue)，消息降级丢弃:queueName:"
                    + queue.getName() + ",messageId:" + message.getMessageId() + ",reason:" + reason);
            purgeMessage(queue, message);
            return false;
        }

        //3. 把原队列侧清干净：unAck + consumerTag追踪 + messageMap + 原队列硬盘文件
        //   messageMap 会被下面 sendMessage 用同一个 messageId 重新登记（同一个对象），先删后插无副作用
        purgeMessage(queue, message);

        //4. 附死信元信息到消息头（客户端消费死信时从 basicProperties.headers 取回）+ 重置重投计数
        //   deliveryCount 必须归零：进死信队列就是"新人"，否则死信队列若也配 x-max-retry，消息一投递就"超限"
        DeadLetterInfo deadLetterInfo = new DeadLetterInfo();
        deadLetterInfo.setReason(reason);
        deadLetterInfo.setCount(1);
        deadLetterInfo.setDeadLetterAt(System.currentTimeMillis());
        deadLetterInfo.setOriginalQueue(virtualHost.stripPrefix(queue.getName()));
        deadLetterInfo.setOriginalRoutingKey(message.getroutingKey());
        BasicProperties properties = message.getBasicProperties();
        Map<String, Object> headers = properties.getHeaders();
        if (headers == null) {
            headers = new HashMap<>();
            properties.setHeaders(headers);
        }
        headers.put(X_DEATH, deadLetterInfo);
        message.setDeliveryCount(0);

        //5. routingKey 改为死信队列名（原 rk 已存入死信元信息，可追溯），
        //   死信若被再次经交换机发布，恰好路由回死信队列
        message.setRoutingKey(dlxName + "_queue");

        //6. 持久化对齐：durable 消息进非 durable 死信队列时降级
        //   （非 durable 队列没有消息文件，writeMessage 会失败）
        message.setdurable(dlQueue.isDurable() && message.getDurable());

        //7. 转投：内存登记 + 硬盘落盘 + notifyConsumer 全在 sendMessage 里
        try {
            virtualHost.sendMessage(dlQueue, message);
            System.out.println("[MessageDisposer] 死信转投成功:原队列:" + queue.getName()
                    + ",死信队列:" + dlQueue.getName() + ",messageId:" + message.getMessageId() + ",reason:" + reason);
            return true;
        } catch (Exception e) {
            System.out.println("[MessageDisposer] 死信转投失败:queueName:" + queue.getName()
                    + ",messageId:" + message.getMessageId() + ",reason:" + reason);
            return false;
        }
    }
}
