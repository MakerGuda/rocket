package org.apache.rocketmq.client;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

@Getter
@Setter
public class QueryResult {

    /**
     * 索引最后一次更新时间
     */
    private final long indexLastUpdateTimestamp;

    /**
     * 消息列表
     */
    private final List<MessageExt> messageList;

    public QueryResult(long indexLastUpdateTimestamp, List<MessageExt> messageList) {
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
        this.messageList = messageList;
    }

    @Override
    public String toString() {
        return "QueryResult [indexLastUpdateTimestamp=" + indexLastUpdateTimestamp + ", messageList=" + messageList + "]";
    }

}