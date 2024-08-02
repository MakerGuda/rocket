package org.apache.rocketmq.common.message;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
public class MessageQueueAssignment implements Serializable {

    private static final long serialVersionUID = 8092600270527861645L;

    private MessageQueue messageQueue;

    private MessageRequestMode mode = MessageRequestMode.PULL;

    private Map<String, String> attachments;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((messageQueue == null) ? 0 : messageQueue.hashCode());
        result = prime * result + ((mode == null) ? 0 : mode.hashCode());
        result = prime * result + ((attachments == null) ? 0 : attachments.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MessageQueueAssignment other = (MessageQueueAssignment) obj;
        return messageQueue.equals(other.messageQueue);
    }

    @Override
    public String toString() {
        return "MessageQueueAssignment [MessageQueue=" + messageQueue + ", Mode=" + mode + "]";
    }

}