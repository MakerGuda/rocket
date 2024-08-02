package org.apache.rocketmq.common.message;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * 消息扩展属性
 */
@Getter
@Setter
public class MessageExt extends Message {

    private static final long serialVersionUID = 5720810158625748049L;

    /**
     * 消息所在broker名称
     */
    private String brokerName;

    /**
     * 消息所在队列id
     */
    private int queueId;

    /**
     * 消息存储大小
     */
    private int storeSize;

    /**
     * 消息在队列中的偏移量
     */
    private long queueOffset;

    /**
     * 系统标识
     */
    private int sysFlag;

    /**
     * 消息生成时间
     */
    private long bornTimestamp;

    private SocketAddress bornHost;

    /**
     * 消息存储时间
     */
    private long storeTimestamp;

    private SocketAddress storeHost;

    private String msgId;

    /**
     * 消息提交偏移量
     */
    private long commitLogOffset;

    private int bodyCRC;

    private int reconsumeTimes;

    private long preparedTransactionOffset;

    public MessageExt() {
    }

    public MessageExt(int queueId, long bornTimestamp, SocketAddress bornHost, long storeTimestamp,
                      SocketAddress storeHost, String msgId) {
        this.queueId = queueId;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.storeTimestamp = storeTimestamp;
        this.storeHost = storeHost;
        this.msgId = msgId;
    }

    public static TopicFilterType parseTopicFilterType(final int sysFlag) {
        if ((sysFlag & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG) {
            return TopicFilterType.MULTI_TAG;
        }
        return TopicFilterType.SINGLE_TAG;
    }

    public static ByteBuffer socketAddress2ByteBuffer(final SocketAddress socketAddress, final ByteBuffer byteBuffer) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        InetAddress address = inetSocketAddress.getAddress();
        if (address instanceof Inet4Address) {
            byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 4);
        } else {
            byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 16);
        }
        byteBuffer.putInt(inetSocketAddress.getPort());
        byteBuffer.flip();
        return byteBuffer;
    }

    public static ByteBuffer socketAddress2ByteBuffer(SocketAddress socketAddress) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        InetAddress address = inetSocketAddress.getAddress();
        ByteBuffer byteBuffer;
        if (address instanceof Inet4Address) {
            byteBuffer = ByteBuffer.allocate(4 + 4);
        } else {
            byteBuffer = ByteBuffer.allocate(16 + 4);
        }
        return socketAddress2ByteBuffer(socketAddress, byteBuffer);
    }

    public ByteBuffer getBornHostBytes() {
        return socketAddress2ByteBuffer(this.bornHost);
    }

    public ByteBuffer getBornHostBytes(ByteBuffer byteBuffer) {
        return socketAddress2ByteBuffer(this.bornHost, byteBuffer);
    }

    public ByteBuffer getStoreHostBytes() {
        return socketAddress2ByteBuffer(this.storeHost);
    }

    public ByteBuffer getStoreHostBytes(ByteBuffer byteBuffer) {
        return socketAddress2ByteBuffer(this.storeHost, byteBuffer);
    }

    public String getBornHostString() {
        if (null != this.bornHost) {
            InetAddress inetAddress = ((InetSocketAddress) this.bornHost).getAddress();
            return null != inetAddress ? inetAddress.getHostAddress() : null;
        }
        return null;
    }

    public void setStoreHostAddressV6Flag() {
        this.sysFlag = this.sysFlag | MessageSysFlag.STOREHOSTADDRESS_V6_FLAG;
    }

    public void setBornHostV6Flag() {
        this.sysFlag = this.sysFlag | MessageSysFlag.BORNHOST_V6_FLAG;
    }

    public Integer getTopicSysFlag() {
        String topicSysFlagString = getProperty(MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG);
        if (topicSysFlagString != null && !topicSysFlagString.isEmpty()) {
            return Integer.valueOf(topicSysFlagString);
        }
        return null;
    }

    public void setTopicSysFlag(Integer topicSysFlag) {
        if (topicSysFlag == null) {
            clearProperty(MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG);
        } else {
            putProperty(MessageConst.PROPERTY_TRANSIENT_TOPIC_CONFIG, String.valueOf(topicSysFlag));
        }
    }

}