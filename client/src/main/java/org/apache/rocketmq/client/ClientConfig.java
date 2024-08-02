package org.apache.rocketmq.client;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RequestType;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * 客户端通用配置
 */
@Getter
@Setter
public class ClientConfig {

    /**
     * 使用vip通道发送消息
     */
    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.rocketmq.sendMessageWithVIPChannel";

    public static final String SOCKS_PROXY_CONFIG = "com.rocketmq.socks.proxy.config";

    public static final String DECODE_READ_BODY = "com.rocketmq.read.body";

    public static final String DECODE_DECOMPRESS_BODY = "com.rocketmq.decompress.body";

    public static final String SEND_LATENCY_ENABLE = "com.rocketmq.sendLatencyEnable";

    public static final String START_DETECTOR_ENABLE = "com.rocketmq.startDetectorEnable";

    public static final String HEART_BEAT_V2 = "com.rocketmq.heartbeat.v2";
    @Deprecated
    protected String namespace;
    protected String namespaceV2;
    protected AccessChannel accessChannel = AccessChannel.LOCAL;
    protected boolean enableStreamRequestType = false;
    protected boolean enableTrace = false;
    protected String traceTopic;
    /**
     * 获取namesrv地址
     */
    private String namesrvAddr = NameServerAddressUtils.getNameServerAddresses();
    /**
     * 获取客户端ip
     */
    private String clientIP = NetworkUtil.getLocalAddress();
    /**
     * 获取客户端实例名称
     */
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    private boolean namespaceInitialized = false;
    /**
     * 从namesrv拉取topic信息的时间间隔
     */
    private int pollNameServerInterval = 1000 * 30;
    /**
     * broker发送心跳检测的时间间隔
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * 持久化消费者组偏移量的时间间隔
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    /**
     * 发生异常时，拉取消息的延迟时间
     */
    private long pullTimeDelayMillsWhenException = 1000;
    private boolean unitMode = false;
    private String unitName;
    private boolean decodeReadBody = Boolean.parseBoolean(System.getProperty(DECODE_READ_BODY, "true"));
    private boolean decodeDecompressBody = Boolean.parseBoolean(System.getProperty(DECODE_DECOMPRESS_BODY, "true"));
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));
    private boolean useHeartbeatV2 = Boolean.parseBoolean(System.getProperty(HEART_BEAT_V2, "false"));
    private boolean useTLS = TlsSystemConfig.tlsEnable;
    private String socksProxyConfig = System.getProperty(SOCKS_PROXY_CONFIG, "{}");
    private int mqClientApiTimeout = 3 * 1000;
    private int detectTimeout = 200;
    private int detectInterval = 2 * 1000;
    private LanguageCode language = LanguageCode.JAVA;
    private boolean sendLatencyEnable = Boolean.parseBoolean(System.getProperty(SEND_LATENCY_ENABLE, "false"));
    private boolean startDetectorEnable = Boolean.parseBoolean(System.getProperty(START_DETECTOR_ENABLE, "false"));
    private boolean enableHeartbeatChannelEventListener = true;

    /**
     * 构建客户端id
     */
    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());
        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }
        if (enableStreamRequestType) {
            sb.append("@");
            sb.append(RequestType.STREAM);
        }
        return sb.toString();
    }

    /**
     * 修改客户端实例名称为进程ip, 当有两个客户端部署在同一台机器上时，不会发生冲突
     */
    public void changeInstanceNameToPID() {
        if (this.instanceName.equals("DEFAULT")) {
            this.instanceName = UtilAll.getPid() + "#" + System.nanoTime();
        }
    }

    public String withNamespace(String resource) {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }

    public Set<String> withNamespace(Set<String> resourceSet) {
        Set<String> resourceWithNamespace = new HashSet<>();
        for (String resource : resourceSet) {
            resourceWithNamespace.add(withNamespace(resource));
        }
        return resourceWithNamespace;
    }

    public String withoutNamespace(String resource) {
        return NamespaceUtil.withoutNamespace(resource, this.getNamespace());
    }

    public Set<String> withoutNamespace(Set<String> resourceSet) {
        Set<String> resourceWithoutNamespace = new HashSet<>();
        for (String resource : resourceSet) {
            resourceWithoutNamespace.add(withoutNamespace(resource));
        }
        return resourceWithoutNamespace;
    }

    public MessageQueue queueWithNamespace(MessageQueue queue) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queue;
        }
        return new MessageQueue(withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId());
    }

    public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queues;
        }
        for (MessageQueue queue : queues) {
            queue.setTopic(withNamespace(queue.getTopic()));
        }
        return queues;
    }

    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInterval = cc.pollNameServerInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.pullTimeDelayMillsWhenException = cc.pullTimeDelayMillsWhenException;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.vipChannelEnabled = cc.vipChannelEnabled;
        this.useTLS = cc.useTLS;
        this.socksProxyConfig = cc.socksProxyConfig;
        this.namespace = cc.namespace;
        this.language = cc.language;
        this.mqClientApiTimeout = cc.mqClientApiTimeout;
        this.decodeReadBody = cc.decodeReadBody;
        this.decodeDecompressBody = cc.decodeDecompressBody;
        this.enableStreamRequestType = cc.enableStreamRequestType;
        this.useHeartbeatV2 = cc.useHeartbeatV2;
        this.startDetectorEnable = cc.startDetectorEnable;
        this.sendLatencyEnable = cc.sendLatencyEnable;
        this.enableHeartbeatChannelEventListener = cc.enableHeartbeatChannelEventListener;
        this.detectInterval = cc.detectInterval;
        this.detectTimeout = cc.detectTimeout;
        this.namespaceV2 = cc.namespaceV2;
        this.enableTrace = cc.enableTrace;
        this.traceTopic = cc.traceTopic;
    }

    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInterval = pollNameServerInterval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        cc.vipChannelEnabled = vipChannelEnabled;
        cc.useTLS = useTLS;
        cc.socksProxyConfig = socksProxyConfig;
        cc.namespace = namespace;
        cc.language = language;
        cc.mqClientApiTimeout = mqClientApiTimeout;
        cc.decodeReadBody = decodeReadBody;
        cc.decodeDecompressBody = decodeDecompressBody;
        cc.enableStreamRequestType = enableStreamRequestType;
        cc.useHeartbeatV2 = useHeartbeatV2;
        cc.startDetectorEnable = startDetectorEnable;
        cc.enableHeartbeatChannelEventListener = enableHeartbeatChannelEventListener;
        cc.sendLatencyEnable = sendLatencyEnable;
        cc.detectInterval = detectInterval;
        cc.detectTimeout = detectTimeout;
        cc.namespaceV2 = namespaceV2;
        cc.enableTrace = enableTrace;
        cc.traceTopic = traceTopic;
        return cc;
    }

    /**
     * 获取namesrv地址
     */
    public String getNamesrvAddr() {
        if (StringUtils.isNotEmpty(namesrvAddr) && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(namesrvAddr.trim()).matches()) {
            return NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(namesrvAddr);
        }
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
        this.namespaceInitialized = false;
    }

    public String getNamespace() {
        if (namespaceInitialized) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(namespace)) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(this.namesrvAddr)) {
            if (NameServerAddressUtils.validateInstanceEndpoint(namesrvAddr)) {
                namespace = NameServerAddressUtils.parseInstanceIdFromEndpoint(namesrvAddr);
            }
        }
        namespaceInitialized = true;
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
        this.namespaceInitialized = true;
    }

    @Override
    public String toString() {
        return "ClientConfig{" +
                "namesrvAddr='" + namesrvAddr + '\'' +
                ", clientIP='" + clientIP + '\'' +
                ", instanceName='" + instanceName + '\'' +
                ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads +
                ", namespace='" + namespace + '\'' +
                ", namespaceInitialized=" + namespaceInitialized +
                ", namespaceV2='" + namespaceV2 + '\'' +
                ", accessChannel=" + accessChannel +
                ", pollNameServerInterval=" + pollNameServerInterval +
                ", heartbeatBrokerInterval=" + heartbeatBrokerInterval +
                ", persistConsumerOffsetInterval=" + persistConsumerOffsetInterval +
                ", pullTimeDelayMillsWhenException=" + pullTimeDelayMillsWhenException +
                ", unitMode=" + unitMode +
                ", unitName='" + unitName + '\'' +
                ", decodeReadBody=" + decodeReadBody +
                ", decodeDecompressBody=" + decodeDecompressBody +
                ", vipChannelEnabled=" + vipChannelEnabled +
                ", useHeartbeatV2=" + useHeartbeatV2 +
                ", useTLS=" + useTLS +
                ", socksProxyConfig='" + socksProxyConfig + '\'' +
                ", mqClientApiTimeout=" + mqClientApiTimeout +
                ", detectTimeout=" + detectTimeout +
                ", detectInterval=" + detectInterval +
                ", language=" + language +
                ", enableStreamRequestType=" + enableStreamRequestType +
                ", sendLatencyEnable=" + sendLatencyEnable +
                ", startDetectorEnable=" + startDetectorEnable +
                ", enableHeartbeatChannelEventListener=" + enableHeartbeatChannelEventListener +
                ", enableTrace=" + enableTrace +
                ", traceTopic='" + traceTopic + '\'' +
                '}';
    }

}