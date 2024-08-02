package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Map;

/**
 * mq管理的基础接口
 */
public interface MQAdmin {

    /**
     * 创建一个主题
     *
     * @param key        accessKey
     * @param newTopic   主题名称
     * @param queueNum   主题的队列数量
     * @param attributes 主题属性
     */
    void createTopic(final String key, final String newTopic, final int queueNum, Map<String, String> attributes) throws MQClientException;

    /**
     * 创建主题
     *
     * @param key          accessKey
     * @param newTopic     主题名称
     * @param queueNum     主题队列数量
     * @param topicSysFlag 主题系统标识
     * @param attributes   主题属性
     */
    void createTopic(String key, String newTopic, int queueNum, int topicSysFlag, Map<String, String> attributes) throws MQClientException;

    /**
     * 根据时间戳获取队列偏移量
     *
     * @param mq        队列
     * @param timestamp 时间戳
     * @return 偏移量
     */
    long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;

    /**
     * 获取队列的最大偏移量
     *
     * @param mq 队列实例
     * @return 最大偏移量
     */
    long maxOffset(final MessageQueue mq) throws MQClientException;

    /**
     * 获取队列的最小偏移量
     *
     * @param mq 队列实例
     * @return 最小偏移量
     */
    long minOffset(final MessageQueue mq) throws MQClientException;

    /**
     * 获取队列中最早存储消息的时间
     *
     * @param mq 队列实例
     * @return 毫秒级别的时间戳
     */
    long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;

    /**
     * 指定主题等信息查询消息
     *
     * @param topic  主题
     * @param key    消息索引键
     * @param maxNum 最大消息数
     * @param begin  起始偏移量
     * @param end    结束偏移量
     * @return 查询结果
     */
    QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin, final long end) throws MQClientException, InterruptedException;

    /**
     * 通过主题和消息id检索消息
     */
    MessageExt viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

}