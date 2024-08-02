package org.apache.rocketmq.remoting.netty;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.common.TlsMode;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

@Getter
@Setter
public class NettyClientConfig {

    /**
     * 客户端工作线程数量
     */
    private int clientWorkerThreads = NettySystemConfig.clientWorkerSize;

    /**
     * 客户端public线程数量
     */
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();

    /**
     * 客户端send oneway最大并发度
     */
    private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;

    /**
     * 客户端异步发送请求最大并发度
     */
    private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;

    /**
     * 连接超时时间
     */
    private int connectTimeoutMillis = NettySystemConfig.connectTimeoutMillis;

    /**
     * channel非活跃时间间隔
     */
    private long channelNotActiveInterval = 1000 * 60;

    /**
     * 是否扫描可用的namesrv
     */
    private boolean isScanAvailableNameSrv = true;

    /**
     * 网络连接最大空闲时间
     */
    private int clientChannelMaxIdleTimeSeconds = NettySystemConfig.clientChannelMaxIdleTimeSeconds;

    /**
     * 客户端发送缓冲区大小
     */
    private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize;

    /**
     * 客户端接收缓冲区大小
     */
    private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

    /**
     * 是否开启ByteBuffer缓存
     */
    private boolean clientPooledByteBufAllocatorEnable = false;

    private boolean clientCloseSocketIfTimeout = NettySystemConfig.clientCloseSocketIfTimeout;

    private boolean useTLS = Boolean.parseBoolean(System.getProperty(TLS_ENABLE, String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING)));

    private String socksProxyConfig = "{}";

    /**
     * 写缓存高水位
     */
    private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;

    /**
     * 写缓存低水位
     */
    private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;

    private boolean disableCallbackExecutor = false;

    private boolean disableNettyWorkerGroup = false;

    /**
     * 重连接最大时间间隔
     */
    private long maxReconnectIntervalTimeSeconds = 60;

    private boolean enableReconnectForGoAway = true;

    private boolean enableTransparentRetry = true;

}