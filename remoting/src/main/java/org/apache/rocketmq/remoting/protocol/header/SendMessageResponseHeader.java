package org.apache.rocketmq.remoting.protocol.header;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;

import java.util.HashMap;

@Getter
@Setter
public class SendMessageResponseHeader implements CommandCustomHeader, FastCodesHeader {

    /**
     * 消息在broker的id
     */
    @CFNotNull
    private String msgId;

    /**
     * 消息所在的队列id
     */
    @CFNotNull
    private Integer queueId;

    /**
     * 消息在队列的偏移量
     */
    @CFNotNull
    private Long queueOffset;

    /**
     * 事务id
     */
    private String transactionId;

    private String batchUniqId;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    @Override
    public void encode(ByteBuf out) {
        writeIfNotNull(out, "msgId", msgId);
        writeIfNotNull(out, "queueId", queueId);
        writeIfNotNull(out, "queueOffset", queueOffset);
        writeIfNotNull(out, "transactionId", transactionId);
        writeIfNotNull(out, "batchUniqId", batchUniqId);
    }

    @Override
    public void decode(HashMap<String, String> fields) throws RemotingCommandException {
        String str = getAndCheckNotNull(fields, "msgId");
        if (str != null) {
            this.msgId = str;
        }
        str = getAndCheckNotNull(fields, "queueId");
        if (str != null) {
            this.queueId = Integer.parseInt(str);
        }
        str = getAndCheckNotNull(fields, "queueOffset");
        if (str != null) {
            this.queueOffset = Long.parseLong(str);
        }
        str = fields.get("transactionId");
        if (str != null) {
            this.transactionId = str;
        }
        str = fields.get("batchUniqId");
        if (str != null) {
            this.batchUniqId = str;
        }
    }

}