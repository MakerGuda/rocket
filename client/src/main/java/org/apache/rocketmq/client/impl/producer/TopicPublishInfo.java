package org.apache.rocketmq.client.impl.producer;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class TopicPublishInfo {

    private boolean orderTopic = false;

    private boolean haveTopicRouterInfo = false;

    private List<MessageQueue> messageQueueList = new ArrayList<>();

    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

    private TopicRouteData topicRouteData;

    public interface QueueFilter {
        boolean filter(MessageQueue mq);
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    /**
     * 根据过滤器筛选mq
     */
    public MessageQueue selectOneMessageQueue(QueueFilter ...filter) {
        return selectOneMessageQueue(this.messageQueueList, this.sendWhichQueue, filter);
    }

    /**
     * 根据broker过滤器，从mq列表中筛选一个mq
     */
    private MessageQueue selectOneMessageQueue(List<MessageQueue> messageQueueList, ThreadLocalIndex sendQueue, QueueFilter ...filter) {
        if (messageQueueList == null || messageQueueList.isEmpty()) {
            return null;
        }
        if (filter != null && filter.length != 0) {
            //遍历所有的mq，存在一个满足所有过滤条件就返回
            for (int i = 0; i < messageQueueList.size(); i++) {
                int index = Math.abs(sendQueue.incrementAndGet() % messageQueueList.size());
                MessageQueue mq = messageQueueList.get(index);
                boolean filterResult = true;
                for (QueueFilter f: filter) {
                    Preconditions.checkNotNull(f);
                    filterResult &= f.filter(mq);
                }
                if (filterResult) {
                    return mq;
                }
            }
            return null;
        }
        //过滤器为空的情况，根据索引计算返回一个
        int index = Math.abs(sendQueue.incrementAndGet() % messageQueueList.size());
        return messageQueueList.get(index);
    }

    /**
     * 重置theadLocal索引
     */
    public void resetIndex() {
        this.sendWhichQueue.reset();
    }

    /**
     * 根据最后一次选中的brokerName筛选mq
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            //遍历，找到一个与上次的brokerName不同的队列
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                MessageQueue mq = selectOneMessageQueue();
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    /**
     * 根据索引返回队列
     */
    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.incrementAndGet();
        int pos = index % this.messageQueueList.size();
        return this.messageQueueList.get(pos);
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

}