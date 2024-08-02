package org.apache.rocketmq.common.message;

import org.apache.rocketmq.common.MixAll;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class MessageBatch extends Message implements Iterable<Message> {

    private static final long serialVersionUID = 621335151046335557L;

    private final List<Message> messages;

    public MessageBatch(List<Message> messages) {
        this.messages = messages;
    }

    public byte[] encode() {
        return MessageDecoder.encodeMessages(messages);
    }

    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    /**
     * 指定消息集合，生成批量消息
     */
    public static MessageBatch generateFromList(Collection<? extends Message> messages) {
        assert messages != null;
        assert !messages.isEmpty();
        List<Message> messageList = new ArrayList<>(messages.size());
        Message first = null;
        for (Message message : messages) {
            //批量消息不支持延迟消息
            if (message.getDelayTimeLevel() > 0) {
                throw new UnsupportedOperationException("TimeDelayLevel is not supported for batching");
            }
            //批量消息不支持重试类型消息
            if (message.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                throw new UnsupportedOperationException("Retry Group is not supported for batching");
            }
            //获取第一条消息
            if (first == null) {
                first = message;
            } else {
                //同一批消息的主题必须一致
                if (!first.getTopic().equals(message.getTopic())) {
                    throw new UnsupportedOperationException("The topic of the messages in one batch should be the same");
                }
                //同一批消息的存储确认机制必须一致
                if (first.isWaitStoreMsgOK() != message.isWaitStoreMsgOK()) {
                    throw new UnsupportedOperationException("The waitStoreMsgOK of the messages in one batch should the same");
                }
            }
            messageList.add(message);
        }
        MessageBatch messageBatch = new MessageBatch(messageList);
        messageBatch.setTopic(first.getTopic());
        messageBatch.setWaitStoreMsgOK(first.isWaitStoreMsgOK());
        return messageBatch;
    }

}